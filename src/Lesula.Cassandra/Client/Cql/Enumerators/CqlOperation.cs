using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    /// <summary>
    /// Codes for CQL messages
    /// </summary>
    /// <remarks>
    /// https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol.spec;hb=refs/heads/cassandra-1.2
    /// </remarks>
    public enum CqlOperation : byte
    {
        Error = 0x00,

        Startup = 0x01,

        Ready = 0x02,

        Authenticate = 0x03,

        Credentials = 0x04,

        Options = 0x05,

        Supported = 0x06,

        Query = 0x07,

        Result = 0x08,

        Prepare = 0x09,

        Execute = 0x0A,

        Register = 0x0B,

        Event = 0x0C,
    }
}
