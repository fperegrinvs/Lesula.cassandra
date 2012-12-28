using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lesula.Cassandra.Connection.Pooling.Impl;
using Lesula.Cassandra.Connection.EndpointManager;
using Lesula.Cassandra.Connection.Factory;

namespace Lesula.Cassandra.Connection.Pooling.Factory
{
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Connection.Pooling.Impl;

    public class NoClientPoolFactory : IFactory<NoClientPool>
    {
        public string Name
        {
            get;
            set;
        }

        public IEndpointManager EndpointManager
        {
            get;
            set;
        }

        public IConnectionFactory ClientFactory
        {
            get;
            set;
        }

        #region IFactory<NoConnectionPool> Members

        public NoClientPool Create()
        {
            NoClientPool pool = new NoClientPool();
            pool.Name = this.Name;
            pool.ClientFactory = this.ClientFactory;
            pool.EndpointManager = this.EndpointManager;
            return pool;
        }

        #endregion
    }
}
