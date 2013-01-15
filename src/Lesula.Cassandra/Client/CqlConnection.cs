// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CqlConnection.cs" company="Lesula MapReduce Framework - http://github.com/lstern/Lesula.cassandra">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   Defines the CqlConnection type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Client.Cql.Exceptions;
    using Lesula.Cassandra.Model;

    /// <summary>
    /// The cql connection.
    /// </summary>
    internal class CqlConnection
    {
        /// <summary>
        ///  A frame has a stream id (one signed byte). When sending request messages, this
        ///  stream id must be set by the client to a positive byte (negative stream id
        ///  are reserved for streams initiated by the server; currently all EVENT messages
        ///  (section 4.2.6) have a streamId of -1). If a client sends a request message
        ///  with the stream id X, it is guaranteed that the stream id of the response to
        ///  that message will be X.
        ///
        ///  This allow to deal with the asynchronous nature of the protocol. If a client
        ///  sends multiple messages simultaneously (without waiting for responses), there
        ///  is no guarantee on the order of the responses. For instance, if the client
        ///  writes REQ_1, REQ_2, REQ_3 on the wire (in that order), the server might
        ///  respond to REQ_3 (or REQ_2) first. Assigning different stream id to these 3
        ///  requests allows the client to distinguish to which request an received answer
        ///  respond to. As there can only be 128 different simultaneous stream, it is up
        ///  to the client to reuse stream id.
        ///
        ///  Note that clients are free to use the protocol synchronously (i.e. wait for
        ///  the response to REQ_N before sending REQ_N+1). In that case, the stream id
        ///  can be safely set to 0. Clients should also feel free to use only a subset of
        ///  the 128 maximum possible stream ids if it is simpler for those
        ///  implementation.
        /// </summary>
        private ConcurrentBag<byte> AvailableStreamIds { get; set; }

        /// <summary>
        /// Request States
        /// </summary>
        private readonly CqlClient[] Clients = new CqlClient[MaxStreams];

        /// <summary>
        /// CQL Header size
        /// </summary>
        private const byte HeaderSize = 8;

        /// <summary>
        /// Maximum possible stream ids
        /// </summary>
        private const byte MaxStreams = 128;

        /// <summary>
        /// The output stream
        /// </summary>
        private Stream Stream { get; set; }

        /// <summary>
        /// Tcp client
        /// </summary>
        private TcpClient TcpClient { get; set; }

        public bool HaveFreeSessions
        {
            get
            {
                return this.AvailableStreamIds.Count > 0;
            }
        }

        public bool IsEmpty
        {
            get
            {
                return this.AvailableStreamIds.Count == MaxStreams;
            }
        }

        /// <summary>
        /// The client endpoint
        /// </summary>
        private IEndpoint Endpoint { get; set; }

        /// <summary>
        /// Configuration
        /// </summary>
        private CqlConfig Config { get; set; }

        public CqlConnection(IEndpoint endpoint, CqlConfig config)
        {
            // Initialize list of available ids
            this.AvailableStreamIds = new ConcurrentBag<byte>();
            for (byte i = 0; i < MaxStreams; i++)
            {
                this.AvailableStreamIds.Add(i);
            }

            this.Config = config;
            this.Endpoint = endpoint;

            this.TcpClient = new TcpClient();
            this.TcpClient.Connect(endpoint.Address, endpoint.Port);

            this.Stream = this.TcpClient.GetStream();

            this.Startup();
            Task.Factory.StartNew(this.Listen);
        }

        /// <summary>
        ///  Initialize the connection. The server will respond by either a READY message
        ///  (in which case the connection is ready for queries) or an AUTHENTICATE message
        ///  (in which case credentials will need to be provided using CREDENTIALS).
        ///
        ///  This must be the first message of the connection, except for OPTIONS that can
        ///  be sent before to find out the options supported by the server. Once the
        ///  connection has been initialized, a client should not send any more STARTUP
        ///  message.
        ///
        ///  The body is a [string map] of options. Possible options are:
        ///    - "CQL_VERSION": the version of CQL to use. This option is mandatory and
        ///      currenty, the only version supported is "3.0.0". Note that this is
        ///      different from the protocol version.
        ///    - "COMPRESSION": the compression algorithm to use for frames (See section 5).
        ///      This is optional, if not specified no compression will be used.
        /// </summary>
        private void Startup()
        {
            var options = new Dictionary<string, string> { { "CQL_VERSION", this.Config.CqlVersion } };

            // say hello
            using (var frameWriter = new FrameWriter(this.Stream, 0))
            {
                frameWriter.SendStartup(options);
            }

            // read response header
            var headerBuffer = new byte[HeaderSize];
            this.Stream.Read(headerBuffer, 0, headerBuffer.Length);
            var header = FrameReader.ProcessHeader(headerBuffer);
            FrameReader.ReadBody(header, this.Stream, this.Config.Streaming);

            bool authenticate;

            switch (header.Operation)
            {
                case CqlOperation.Ready:
                    authenticate = false;
                    break;
                case CqlOperation.Credentials:
                    authenticate = true;
                    break;
                default:
                    throw new UnknownResponseException(header.Operation);
            }

            if (authenticate)
            {
                throw new NotImplementedException("Authentication not supported yet!");
            }
        }

        /// <summary>
        /// Get which stream is active
        /// </summary>
        private async void Listen()
        {
            while (true)
            {
                var headerBuffer = new byte[HeaderSize];
                await this.Stream.ReadAsync(headerBuffer, 0, headerBuffer.Length);
                var header = FrameReader.ProcessHeader(headerBuffer);
                this.RouteMessage(header);
            }
        }

        private void RouteMessage(CqlMessageHeader messageHeader)
        {
            var client = this.Clients[messageHeader.StreamId];
            client.ReadResponse(messageHeader);
        }

        public CqlClient GetClient()
        {
            byte streamId;
            while (!this.AvailableStreamIds.TryTake(out streamId))
            {
                Thread.Sleep(50);
            }

            var client = new CqlClient(streamId, this.Stream);
            client.Available += () => this.AvailableStreamIds.Add(streamId);
            this.Clients[streamId] = client;
            return client;
        }
    }
}
