namespace Lesula.Cassandra
{
    using Apache.Cassandra;

    public delegate T ExecutionBlock<T>(Cassandra.Iface client);
}
