// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    using System;

    /// <summary>
    ///  The version is a single byte that indicate both the direction of the message
    ///  (request or response) and the version of the protocol in use. The up-most bit
    ///  of version is used to define the direction of the message: 0 indicates a
    ///  request, 1 indicates a responses. This can be useful for protocol analyzers to
    ///  distinguish the nature of the packet from the direction which it is moving.
    ///  The rest of that byte is the protocol version (1 for the protocol defined in
    ///  this document). In other words, for this version of the protocol, version will
    ///  have one of:
    ///    0x01    Request frame for this protocol version
    ///    0x81    Response frame for this protocol version
    /// </summary>
    /// <remarks>
    /// Taken from https://github.com/pchalamet/cassandra-sharp
    /// </remarks>
    [Flags]
    internal enum FrameType
    {
        ProtocolVersion = 0x01,

        ProtocolVersionMask = 0x7F,

        Request = 0x00,

        Response = 0x80,
    }
}