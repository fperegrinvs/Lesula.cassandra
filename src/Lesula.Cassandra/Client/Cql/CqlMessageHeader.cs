namespace Lesula.Cassandra.Client.Cql
{
    using Lesula.Cassandra.Client.Cql.Enumerators;

    public class CqlMessageHeader
    {
        public byte Version { get; set; }

        public MessageDirection Direction { get; set; }

        public CqlHeaderFlags Flags { get; set; }

        public byte StreamId { get; set; }

        public CqlOperation Operation { get; set; }

        public int Size { get; set; }
    }
}
