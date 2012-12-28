namespace Lesula.Cassandra.Connection.Factory
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    using Thrift.Protocol;
    using Thrift.Transport;

    using CassandraClient = Apache.Cassandra.Cassandra.Client;

    public class BufferedTransportConnectionFactory : IConnectionFactory
    {

        private int bufferSize;
        private bool isBufferSizeSet;

        public int BufferSize
        {
            get { return bufferSize; }
            set
            {
                this.bufferSize = value;
                this.isBufferSizeSet = true;
            }
        }

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

            if (this.isBufferSizeSet)
            {
                transport = new TBufferedTransport(socket, this.bufferSize);
            }
            else
            {
                transport = new TBufferedTransport(socket);
            }

            TProtocol protocol = new TBinaryProtocol(transport);
            CassandraClient cassandraClient = new CassandraClient(protocol);
            IClient client = new DefaultClient() {
                CassandraClient = cassandraClient,
                Endpoint = endpoint,
                OwnerPool = ownerPool
            };

            return client;
        }

        #endregion
    }
}
