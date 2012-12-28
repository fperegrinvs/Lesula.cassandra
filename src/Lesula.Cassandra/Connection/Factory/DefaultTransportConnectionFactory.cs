namespace Lesula.Cassandra.Connection.Factory
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    using Thrift.Protocol;
    using Thrift.Transport;

    using CassandraClient = Apache.Cassandra.Cassandra.Client;

    public class DefaultTransportConnectionFactory : IConnectionFactory
    {
        #region IConnectionFactory Members

        public IClient Create(IEndpoint endpoint, IClientPool ownerPool)
        {
            TTransport transport = null;
            if (endpoint.Timeout == 0)
            {
                transport = new TSocket(endpoint.Address, endpoint.Port);
            }
            else
            {
                transport = new TSocket(endpoint.Address, endpoint.Port, endpoint.Timeout);
            }
            TProtocol protocol = new TBinaryProtocol(transport);
            CassandraClient cassandraClient = new CassandraClient(protocol);
            IClient client = new DefaultClient()
            {
                CassandraClient = cassandraClient,
                Endpoint = endpoint,
                OwnerPool = ownerPool
            };

            return client;
        }

        #endregion
    }
}
