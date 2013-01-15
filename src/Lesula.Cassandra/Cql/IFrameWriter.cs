// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IFrameWriter.cs" company="Lesula MapReduce Framework - http://github.com/lstern/Lesula.cassandra">
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
//   Defines the IFrameWriter type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql
{
    using System.Collections.Generic;

    using Lesula.Cassandra.Client.Cql.Enumerators;

    /// <remarks>
    /// Adapted from https://github.com/pchalamet/cassandra-sharp 
    /// </remarks>
    public interface IFrameWriter
    {
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
        void SendQuery(string cql, CqlConsistencyLevel cl, CqlOperation opcode);

        /// <summary>
        ///  Asks the server to return what STARTUP options are supported. The body of an
        ///  OPTIONS message should be empty and the server will respond with a SUPPORTED
        ///  message.
        /// </summary>
        void SendOptions();

        /// <summary>
        ///  Prepare a query for later execution (through EXECUTE). The body consists of
        ///  the CQL query to prepare as a [long string].
        ///
        ///  The server will respond with a RESULT message with a `prepared` kind (0x00003,
        ///  see Section 4.2.5). 
        /// </summary>
        void SendPrepareRequest(string cql);

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
        void SendStartup(Dictionary<string, string> options);

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
        void SendCredentials(string user, string password);

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
        void SendExecutePreparedQuery(byte[] id, short n, List<byte[]> values, CqlConsistencyLevel cl);
    }
}