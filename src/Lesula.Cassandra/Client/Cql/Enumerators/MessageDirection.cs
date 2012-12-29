namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    public enum MessageDirection : byte
    {
        Request = 0x00,

        Response = 0x80
    }
}
