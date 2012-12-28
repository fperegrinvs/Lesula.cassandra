namespace Lesula.Cassandra.Connection.Factory
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Client.Fake;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public class FakeConnectionFactory : IConnectionFactory
    {
        #region IConnectionFactory Members

        public IClient Create(IEndpoint endpoint, IClientPool ownerPool)
        {
            IClient client = new FakeClient
            {
                CassandraClient = new FakeCassandra(),
                Endpoint = endpoint,
                OwnerPool = ownerPool
            };

            return client;
        }

        #endregion
    }
}
