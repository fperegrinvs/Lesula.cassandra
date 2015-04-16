namespace Lesula.Cassandra.Connection.Pooling
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Configuration;
    using Lesula.Cassandra.Model;
    using System.Collections.Generic;

    public interface IClientPool
    {
        IClient Borrow(string keyspace = null);
        void Release(IClient client);
        void Invalidate(IClient client);

        List<IEndpoint> EndPoints { get; }
    }
}
