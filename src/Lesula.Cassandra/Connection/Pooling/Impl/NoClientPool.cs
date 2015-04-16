using Lesula.Cassandra.Connection.Factory;
using Lesula.Cassandra.Connection.EndpointManager;
using Lesula.Cassandra.Model;
using System.Diagnostics;

namespace Lesula.Cassandra.Connection.Pooling.Impl
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Model;
    using System.Collections.Generic;

    public class NoClientPool : IClientPool
    {
        public IEndpointManager EndpointManager
        {
            set;
            get;
        }

        public IConnectionFactory ClientFactory
        {
            set;
            get;
        }

        public List<IEndpoint> EndPoints { get; set; }

        #region IConnectionPool Members

        public string Name { get; set; }

        public IClient Borrow(string keyspace = null)
        {
            IClient client = null;

            IEndpoint endpoint = this.EndpointManager.Pick();
            if (endpoint != null)
            {
                client = this.ClientFactory.Create(endpoint, this);
                client.Open();
            }
            return client;
        }

        public void Release(IClient client)
        {
            client.Close();
        }

        public void Invalidate(IClient client)
        {
            client.Close();
        }

        #endregion
    }
}
