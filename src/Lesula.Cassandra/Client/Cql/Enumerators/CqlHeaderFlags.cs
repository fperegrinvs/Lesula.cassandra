// --------------------------------------------------------------------------------------------------------------------
// <copyright file="FrameHeaderFlags.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    using System;

    /// <summary>
    ///  Flags applying to this frame. The flags have the following meaning (described
    ///  by the mask that allow to select them):
    ///    0x01: Compression flag. If set, the frame body is compressed. The actual
    ///          compression to use should have been set up beforehand through the
    ///          Startup message (which thus cannot be compressed; Section 4.1.1).
    ///    0x02: Tracing flag. For a request frame, this indicate the client requires
    ///          tracing of the request. Note that not all requests support tracing.
    ///          Currently, only QUERY, PREPARE and EXECUTE queries support tracing.
    ///          Other requests will simply ignore the tracing flag if set. If a
    ///          request support tracing and the tracing flag was set, the response to
    ///          this request will have the tracing flag set and contain tracing
    ///          information.
    ///          If a response frame has the tracing flag set, its body contains
    ///          a tracing ID. The tracing ID is a [uuid] and is the first thing in
    ///          the frame body. The rest of the body will then be the usual body
    ///          corresponding to the response opcode.
    ///
    ///  The rest of the flags is currently unused and ignored.
    /// </summary>
    [Flags]
    public enum CqlHeaderFlags : byte
    {
        None = 0x00,

        Compression = 0x01,

        Tracing = 0x02,
    }
}