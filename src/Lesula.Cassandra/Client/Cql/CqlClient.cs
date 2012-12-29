namespace Lesula.Cassandra.Client.CQL
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    using Lesula.Cassandra.Client.Cql;
    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Client.Cql.Exceptions;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public class CqlClient : AbstractClient
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
        private readonly RequestState[] RequestStates = new RequestState[MaxStreams];

        /// <summary>
        /// Maximum possible stream ids
        /// </summary>
        private const byte MaxStreams = 128;

        /// <summary>
        /// The input stream
        /// </summary>
        private Stream InputStream { get; set; }

        /// <summary>
        /// The output stream
        /// </summary>
        private Stream OutputStream { get; set; }

        /// <summary>
        /// Tcp client
        /// </summary>
        private TcpClient TcpClient { get; set; }

        /// <summary>
        /// The client endpoint
        /// </summary>
        private IEndpoint Endpoint { get; set; }

        /// <summary>
        /// The owner pool
        /// </summary>
        private IClientPool OwnerPool { get; set; }

        private CqlConfig Config { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlClient"/> class.
        /// </summary>
        /// <param name="endpoint">
        /// The endpoint.
        /// </param>
        /// <param name="ownerPool">
        /// The owner pool.
        /// </param>
        /// <param name="config">
        /// The config.
        /// </param>
        public CqlClient(IEndpoint endpoint, IClientPool ownerPool, CqlConfig config)
        {
            // Initialize list of available ids
            this.AvailableStreamIds = new ConcurrentBag<byte>();
            for (byte i = 0; i < MaxStreams; i++)
            {
                this.AvailableStreamIds.Add(i);
                this.RequestStates[i].Lock = new object();
            }

            this.Config = config;
            this.Endpoint = endpoint;
            this.OwnerPool = ownerPool;
            this.TcpClient = new TcpClient();
            this.TcpClient.Connect(endpoint.Address, endpoint.Port);

            Stream stream = this.TcpClient.GetStream();
            this.InputStream = stream;
            this.OutputStream = stream;

            this.Startup();
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
        /// <returns></returns>
        private void Startup()
        {
            Action<IFrameWriter> writer = fw => WriteReady(fw, this.Config.CqlVersion);
            Func<IFrameReader, IEnumerable<object>> reader = fr => new object[] { ReadReady(fr) };
            bool authenticate = this.Execute(writer, reader).Result.Cast<bool>().Single();
            if (authenticate)
            {
                throw new NotImplementedException("Authentication not supported yet!");
            }
        }

        internal static bool ReadReady(IFrameReader frameReader)
        {
            switch (frameReader.MessageOpcode)
            {
                case CqlOperation.Ready:
                    return false;

                case CqlOperation.Credentials:
                    return true;

                default:
                    throw new UnknownResponseException(frameReader.MessageOpcode);
            }
        }

        internal static void WriteReady(IFrameWriter frameWriter, string cqlVersion)
        {
            var options = new Dictionary<string, string> { { "CQL_VERSION", cqlVersion } };

            frameWriter.WriteStringMap(options);
            frameWriter.Send(CqlOperation.Startup);
        }

        public Task<IEnumerable<object>> Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader)
        {
            byte streamId;
            while (!this.AvailableStreamIds.TryTake(out streamId))
            {
                Thread.Sleep(100);
            }

            // startup a new read task
            //_currReadTask = null != _currReadTask
            //                        ? _currReadTask.ContinueWith(_ => ReadNextFrameHeader())
            //                        : 
            Task.Factory.StartNew(this.ReadNextFrameHeader);

            // start the async request
            var taskWrite = Task.Factory.StartNew(() => this.WriteNextFrame(writer, reader, streamId));
            return taskWrite;
        }

        private void ReadNextFrameHeader()
        {
            try
            {
                // read stream id - we are the only one reading so no lock required
                byte streamId = FrameReader.ReadStreamId(this.InputStream);

                // acquire request lock
                lock (this.RequestStates[streamId].Lock)
                {
                    // flip the status flag (write barrier in Pulse below)
                    this.RequestStates[streamId].ReadBegan = true;

                    // hand off the reading of the body to the request handler
                    Monitor.Pulse(this.RequestStates[streamId].Lock);

                    // wait for request handler to complete
                    Monitor.Wait(this.RequestStates[streamId].Lock);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private IEnumerable<object> StreamResultsThenReleaseStreamId(Func<FrameReader, IEnumerable<object>> reader, byte streamId)
        {
            // we are completely client side there (ie: running on the thread of the client)
            // we have first to grab the request lock to avoid a race with async reader
            // and find if the async reader has started to read the frame
            lock (this.RequestStates[streamId].Lock)
            {
                try
                {
                    // if the reader has not read this stream id then just wait for a notification
                    if (!this.RequestStates[streamId].ReadBegan)
                    {
                        Monitor.Wait(this.RequestStates[streamId].Lock);
                    }

                    // release stream id (since result streaming has started)
                    this.AvailableStreamIds.Add(streamId);


                    // yield all rows - no lock required on input stream since we are the only one allowed to read
                    using (FrameReader frameReader = FrameReader.ReadBody(this.InputStream, this.Config.Streaming))
                    {
                        foreach (object row in reader(frameReader) ?? Enumerable.Empty<object>())
                        {
                            yield return row;
                        }
                    }
                }
                finally
                {
                    // wake up the async reader
                    this.RequestStates[streamId].ReadBegan = false;
                    Monitor.Pulse(this.RequestStates[streamId].Lock);
                }
            }
        }

        private IEnumerable<object> WriteNextFrame(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, byte streamId)
        {


            using (FrameWriter frameWriter = new FrameWriter(this.OutputStream, streamId))
                writer(frameWriter);


            // return a promise to stream results
            return StreamResultsThenReleaseStreamId(reader, streamId);
        }

        #region Overrides of AbstractClient

        public override string KeyspaceName { get; set; }

        private bool isOpen = false;

        public override void Open()
        {
            this.isOpen = true;
        }

        public override void Close()
        {
            throw new System.NotImplementedException();
        }

        public override bool IsOpen()
        {
            return this.isOpen;
        }

        public override T Execute<T>(ExecutionBlock<T> executionBlock)
        {
            throw new System.NotImplementedException();
        }

        public override string getClusterName()
        {
            throw new System.NotImplementedException();
        }

        #endregion

        private struct RequestState
        {
            public object Lock;

            public bool ReadBegan;
        }
    }

    public class CqlConfig
    {
        public int Port { get; set; }

        public string Type { get; set; }

        public bool Recoverable { get; set; }

        public string User { get; set; }

        public string Password { get; set; }

        public string CqlVersion { get; set; }

        public bool Streaming { get; set; }
    }
}
