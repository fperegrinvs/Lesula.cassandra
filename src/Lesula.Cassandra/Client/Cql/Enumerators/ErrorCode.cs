// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ErrorCode.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   The error code.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    /// <summary>
    /// The error code.
    /// </summary>
    public enum ErrorCode
    {
        /// <summary>
        /// Server error: something unexpected happened. This indicates a server-side bug.
        /// </summary>
        Server = 0x0000,

        /// <summary>
        /// Protocol error: some client message triggered a protocol
        /// violation (for instance a QUERY message is sent before a STARTUP
        /// one has been sent)
        /// </summary>
        Protocol = 0x000A,

        /// <summary>
        /// Bad credentials: CREDENTIALS request failed because Cassandra
        /// did not accept the provided credentials.
        /// </summary>
        BadCredentials = 0x0100,

        /// <summary>
        /// Unavailable exception. The rest of the ERROR message body will be
        ///   &lt;cl&gt;&lt;required&gt;&lt;alive&gt;
        /// where:
        ///   &lt;cl&gt; is the [consistency] level of the query having triggered
        ///        the exception.
        ///   &lt;required&gt; is an [int] representing the number of node that
        ///              should be alive to respect &lt;cl&gt;
        ///   &lt;alive&gt; is an [int] representing the number of replica that
        ///           were known to be alive when the request has been
        ///           processed (since an unavailable exception has been
        ///           triggered, there will be &lt;alive&gt; &lt; &lt;required&gt;)
        /// </summary>
        Unavailable = 0x1000,

        /// <summary>
        /// Overloaded: the request cannot be processed because the
        /// coordinator node is overloaded
        /// </summary>
        Overloaded = 0x1001,

        /// <summary>
        /// Is_bootstrapping: the request was a read request but the
        /// coordinator node is bootstrapping
        /// </summary>
        IsBootstrapping = 0x1002,

        /// <summary>
        /// Truncate_error: error during a truncation error.
        /// </summary>
        Truncate = 0x1003,

        /// <summary>
        /// Write_timeout: Timeout exception during a write request. The rest
        /// of the ERROR message body will be
        ///   &lt;cl&gt;&lt;received&gt;&lt;blockfor&gt;&lt;writeType&gt;
        /// where:
        ///   &lt;cl&gt; is the [consistency] level of the query having triggered
        ///        the exception.
        ///   &lt;received&gt; is an [int] representing the number of nodes having
        ///              acknowledged the request.
        ///   &lt;blockfor&gt; is the number of replica whose acknowledgement is
        ///              required to achieve &lt;cl&gt;.
        ///   &lt;writeType&gt; is a [string] that describe the type of the write
        ///               that timeouted. The value of that string can be one
        ///               of:
        ///                - "SIMPLE": the write was a non-batched
        ///                  non-counter write.
        ///                - "BATCH": the write was a (logged) batch write.
        ///                  If this type is received, it means the batch log
        ///                  has been successfully written (otherwise a
        ///                  "BATCH_LOG" type would have been send instead).
        ///                - "UNLOGGED_BATCH": the write was an unlogged
        ///                  batch. Not batch log write has been attempted.
        ///                - "COUNTER": the write was a counter write
        ///                  (batched or not).
        ///                - "BATCH_LOG": the timeout occured during the
        ///                  write to the batch log when a (logged) batch
        ///                  write was requested.        
        /// </summary>
        WriteTimeout = 0x1100,

        /// <summary>
        /// Read_timeout: Timeout exception during a read request. The rest
        /// of the ERROR message body will be
        ///   &lt;cl&gt;&lt;received&gt;&lt;blockfor&gt;&lt;data_present&gt;
        /// where:
        ///   &lt;cl&gt; is the [consistency] level of the query having triggered
        ///        the exception.
        ///   &lt;received&gt; is an [int] representing the number of nodes having
        ///              answered the request.
        ///   &lt;blockfor&gt; is the number of replica whose response is
        ///              required to achieve &lt;cl&gt;. Please note that it is
        ///              possible to have &lt;received&gt; &gt;= &lt;blockfor&gt; if
        ///              &lt;data_present&gt; is false. And also in the (unlikely)
        ///              case were &lt;cl&gt; is achieved but the coordinator node
        ///              timeout while waiting for read-repair
        ///              acknowledgement.
        ///   &lt;data_present&gt; is a single byte. If its value is 0, it means
        ///                  the replica that was asked for data has not
        ///                  responded. Otherwise, the value is != 0.
        /// </summary>
        ReadTimeout = 0x1200,

        /// <summary>
        /// Syntax_error: The submitted query has a syntax error.
        /// </summary>
        Syntax = 0x2000,

        /// <summary>
        /// Unauthorized: The logged user doesn't have the right to perform
        /// the query.
        /// </summary>
        Unauthorized = 0x2100,

        /// <summary>
        /// Invalid: The query is syntactically correct but invalid.
        /// </summary>
        Invalid = 0x2200,

        /// <summary>
        /// Config_error: The query is invalid because of some configuration issue.
        /// </summary>
        Config = 0x2300,

        /// <summary>
        /// Already_exists: The query attempted to create a keyspace or a
        /// table that was already existing. The rest of the ERROR message
        /// body will be &lt;ks&gt;&lt;table&gt; where:
        ///   &lt;ks&gt; is a [string] representing either the keyspace that
        ///        already exists, or the keyspace in which the table that
        ///        already exists is.
        ///   &lt;table&gt; is a [string] representing the name of the table that
        ///           already exists. If the query was attempting to create a
        ///           keyspace, &lt;table&gt; will be present but will be the empty
        ///           string.
        /// </summary>
        AlreadyExists = 0x2400,

        /// <summary>
        /// Unprepared: Can be thrown while a prepared statement tries to be
        /// executed if the provide prepared statement ID is not known by
        /// this host.  The rest of the ERROR message body will be [short bytes] representing the unknown ID.
        /// </summary>
        Unprepared = 0x2500,

        /// <summary>
        /// Unkown error
        /// </summary>
        Unknown = 0xFFFF,
    }
}
