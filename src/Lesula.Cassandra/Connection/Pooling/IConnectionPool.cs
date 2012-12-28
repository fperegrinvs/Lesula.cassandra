namespace Lesula.Cassandra.Connection.Pooling
{
    using Lesula.Cassandra.Client;

    public interface IClientPool
    {
        IClient Borrow(string keyspace = null);
        void Release(IClient client);
        void Invalidate(IClient client);
    }
}
