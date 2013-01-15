// --------------------------------------------------------------------------------------------------------------------
// <copyright file="FrameWriter.cs" company="Lesula MapReduce Framework - http://github.com/lstern/Lesula.cassandra">
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
//   Defines the FrameWriter type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Client.Cql.Extensions;

    /// <remarks>
    /// Adapted from https://github.com/pchalamet/cassandra-sharp 
    /// </remarks>
    internal class FrameWriter : IFrameWriter, IDisposable
    {
        private readonly Stream stream;

        private readonly byte streamId;

        private readonly MemoryStream ms;

        internal FrameWriter(Stream stream, byte streamId)
        {
            this.stream = stream;
            this.streamId = streamId;
            this.ms = new MemoryStream();
        }

        public void Dispose()
        {
            this.stream.Flush();
            this.ms.SafeDispose();
        }

        /// <summary>
        ///  Performs a CQL query. The body of the message consists of a CQL query as a [long
        ///  string] followed by the [consistency] for the operation.
        ///
        ///  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
        ///  TRUNCATE, ...).
        ///
        ///  The server will respond to a QUERY message with a RESULT message, the content
        ///  of which depends on the query.
        /// </summary>
        public void SendQuery(string cql, CqlConsistencyLevel cl, CqlOperation opcode)
        {
            this.WriteLongString(cql);
            this.WriteShort((short)cl);
            this.Send(opcode);
        }

        /// <summary>
        ///  Asks the server to return what STARTUP options are supported. The body of an
        ///  OPTIONS message should be empty and the server will respond with a SUPPORTED
        ///  message.
        /// </summary>
        public void SendOptions()
        {
            this.Send(CqlOperation.Options);
        }

        /// <summary>
        ///  Prepare a query for later execution (through EXECUTE). The body consists of
        ///  the CQL query to prepare as a [long string].
        ///
        ///  The server will respond with a RESULT message with a `prepared` kind (0x00003,
        ///  see Section 4.2.5). 
        /// </summary>
        public void SendPrepareRequest(string cql)
        {
            this.WriteLongString(cql);
            this.Send(CqlOperation.Prepare);
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
        public void SendStartup(Dictionary<string, string> options)
        {
            this.WriteStringMap(options);
            this.Send(CqlOperation.Startup);
        }

        /// <summary>
        ///  Provides credentials information for the purpose of identification. This
        ///  message comes as a response to an AUTHENTICATE message from the server, but
        ///  can be use later in the communication to change the authentication
        ///  information.
        ///
        ///  The body is a list of key/value informations. It is a [short] n, followed by n
        ///  pair of [string]. These key/value pairs are passed as is to the Cassandra
        ///  IAuthenticator and thus the detail of which informations is needed depends on
        ///  that authenticator.
        ///
        ///  The response to a CREDENTIALS is a READY message (or an ERROR message).        
        /// </summary>
        public void SendCredentials(string user, string password)
        {
            var authParams = new[] { user, password };
            this.WriteStringList(authParams);
            this.Send(CqlOperation.Credentials);
        }

        /// <summary>
        ///          Executes a prepared query. The body of the message must be:
        ///    &lt;id&gt;&lt;n&gt;&lt;value_1&gt;....&lt;value_n&gt;&lt;consistency&gt;
        ///  where:
        ///    - &lt;id&gt; is the prepared query ID. It's the [short bytes] returned as a
        ///      response to a PREPARE message.
        ///    - &lt;n&gt; is a [short] indicating the number of following values.
        ///    - &lt;value_1&gt;...&lt;value_n&gt; are the [bytes] to use for bound variables in the
        ///      prepared query.
        ///    - &lt;consistency&gt; is the [consistency] level for the operation.
        ///
        ///  Note that the consistency is ignored by some (prepared) queries (USE, CREATE,
        ///  ALTER, TRUNCATE, ...).
        ///
        ///  The response from the server will be a RESULT message.
        /// </summary>
        public void SendExecutePreparedQuery(byte[] id, short n, List<byte[]> values, CqlConsistencyLevel cl)
        {
            this.WriteShortByteArray(id);
            this.WriteShort(n);
            foreach (var value in values)
            {
                this.WriteByteArray(value);
            }

            this.WriteShort((short)cl);
            this.Send(CqlOperation.Execute);
        }

        protected void Send(CqlOperation msgOpcode)
        {
            var version = (byte)((byte)MessageDirection.Request | FrameReader.ProtocolVersion);
            this.stream.WriteByte(version);

            const byte Flags = (byte)CqlHeaderFlags.None;

            this.stream.WriteByte(Flags);

            // streamId
            this.stream.WriteByte(this.streamId);

            // opcode
            this.stream.WriteByte((byte)msgOpcode);

            // len of body
            var bodyLen = (int)this.ms.Length;
            this.stream.WriteInt(bodyLen);

            // body
            this.stream.Write(this.ms.GetBuffer(), 0, bodyLen);
            this.stream.Flush();
        }

        public void WriteShort(short data)
        {
            this.ms.WriteShort(data);
        }

        public void WriteInt(int data)
        {
            this.ms.WriteInt(data);
        }

        public void WriteString(string data)
        {
            this.ms.WriteString(data);
        }

        public void WriteShortByteArray(byte[] data)
        {
            this.ms.WriteShortByteArray(data);
        }

        public void WriteLongString(string data)
        {
            this.ms.WriteLongString(data);
        }

        public void WriteStringMap(Dictionary<string, string> dic)
        {
            this.ms.WriteStringMap(dic);
        }

        public void WriteStringList(string[] data)
        {
            this.ms.WriteStringList(data);
        }

        public void WriteByteArray(byte[] data)
        {
            this.ms.WriteByteArray(data);
        }
    }
}