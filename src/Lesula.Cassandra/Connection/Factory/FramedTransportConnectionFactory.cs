namespace Lesula.Cassandra.Connection.Factory
{
    using Apache.Cassandra;

    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    using Thrift.Protocol;
    using Thrift.Transport;

    public class FramedTransportConnectionFactory : IConnectionFactory
    {
        #region IConnectionFactory Members

        public IClient Create(IEndpoint endpoint, IClientPool ownerPool)
        {
            TSocket socket = null;
            TTransport transport = null;
            if (endpoint.Timeout == 0)
            {
                socket = new TSocket(endpoint.Address, endpoint.Port);
            }
            else
            {
                socket = new TSocket(endpoint.Address, endpoint.Port, endpoint.Timeout);
            }

            transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client cassandraClient = new Cassandra.Client(protocol);
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
