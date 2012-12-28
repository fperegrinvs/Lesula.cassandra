namespace Lesula.Cassandra.Tests.Core
{
    using System;
    using System.Collections.Generic;

    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Cluster.Impl;
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Connection.Pooling.Impl;
    using Lesula.Cassandra.Model;
    using Lesula.Cassandra.Model.Impl;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the default cluster
    /// </summary>
    [TestClass]
    public class DefaultClusterTest
    {
        [TestMethod]
        public void DefaultClusterTestOne()
        {
            var cluster = new DefaultCluster();
            cluster.MaximumRetries = 1;
            cluster.Name = "TestCluster";
            var pool = new SizeControlledClientPool();
            pool.ClientFactory = new MyClientFactory();
            pool.DueTime = 2000;
            var endpointManager = new MyEndpointManager();
            endpointManager.Name = "Endpoint";
            endpointManager.Endpoints = new List<IEndpoint>();
            endpointManager.Endpoints.Add(new DefaultEndpoint() { Address = "127.0.0.1", Port = 9160, Timeout = 1000 });
            pool.EndpointManager = endpointManager;
            pool.MagicNumber = 7;
            pool.MaximumClientsToSupport = 10;
            pool.MaximumRetriesToPollClient = 1;
            pool.MinimumClientsToKeep = 1;
            pool.PeriodicTime = 2000;
            cluster.PoolManager = pool;

            for (int i = 0; i < 100; i++)
            {
                IClient client = cluster.Borrow();
                cluster.Release(client);
            }
        }
    }

    public class MyClientFactory : IConnectionFactory
    {
        #region IConnectionFactory Members

        public IClient Create(IEndpoint endpoint, Connection.Pooling.IClientPool ownerPool)
        {
            return new MyClient { Endpoint = endpoint, OwnerPool = ownerPool };
        }

        #endregion
    }

    public class MyClient : AbstractClient
    {
        public override T Execute<T>(ExecutionBlock<T> executionBlock)
        {
            return default(T);
        }

        public override string getClusterName()
        {
            return String.Empty;
        }

        public override void Close()
        {
            // DO NOTHING
        }

        public override bool IsOpen()
        {
            return true;
        }

        public override void Open()
        {
            // DO NOTHING
        }

        public override string KeyspaceName
        {
            get;
            set;
        }
    }

    public class MyEndpointManager : IEndpointManager
    {
        #region IEndpointManager Members

        public List<IEndpoint> Endpoints { get; set; }

        public string Name { get; set; }

        public IEndpoint Pick()
        {
            return this.Endpoints[0];
        }

        public void Ban(IEndpoint endpoint)
        {
        }

        #endregion
    }
}
