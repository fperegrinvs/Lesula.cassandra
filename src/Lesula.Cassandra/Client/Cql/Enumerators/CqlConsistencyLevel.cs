namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    /// <summary>
    ///  A consistency level specification.
    /// </summary>
    public enum CqlConsistencyLevel : short
    {
        ANY = 0x0000,

        ONE = 0x0001,

        TWO = 0x0002,

        THREE = 0x0003,

        QUORUM = 0x0004,

        ALL = 0x0005,

        LOCAL_QUORUM = 0x0006,

        EACH_QUORUM = 0x0007,
    }
}