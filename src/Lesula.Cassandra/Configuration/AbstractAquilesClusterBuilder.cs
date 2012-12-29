// --------------------------------------------------------------------------------------------------------------------
// <copyright file="AbstractAquilesClusterBuilder.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   Defines the AbstractAquilesClusterBuilder type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.Cluster.Factory;
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.EndpointManager.Factory;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Connection.Pooling.Factory;
    using Lesula.Cassandra.Connection.Pooling.Impl;
    using Lesula.Cassandra.Extensions;
    using Lesula.Cassandra.Model;
    using Lesula.Cassandra.Model.Impl;

    public abstract class AbstractAquilesClusterBuilder
    {
        public enum ClusterType
        {
            /// <summary>
            /// Thrift protocol (default)
            /// </summary>
            Thrift,

            /// <summary>
            /// CQL Binary protocol (Cassandra 1.2+)
            /// </summary>
            Cql,
        }

        /// <summary>
        /// Type of ConnectionPool
        /// </summary>
        public enum PoolType
        {
            /// <summary>
            /// No pool is used. Clients are created based on need, they are disposed as soon as they are returned.
            /// </summary>
            NoPool = 0,

            /// <summary>
            /// Warmup enable and size-controlled enabled pool.
            /// </summary>
            SizeControlledPool = 1,

            /// <summary>
            /// Warmup enable and size-controlled enabled pool, divided by keyspaces.
            /// </summary>
            SizedKeyspacePool = 2,

            /// <summary>
            /// Cql binary protocol with pooling
            /// </summary>
            CqlPool = 3,
        }

        public enum EndpointManagerType
        {
            /// <summary>
            /// Cycle through the list of endpoints to balance the pool connections
            /// </summary>
            RoundRobin,
        }

        protected const string EndpointmanagerDuetimeKey = "endpointManagerDueTime";
        protected const string EndpointmanagerPeriodictimeKey = "endpointManagerPeriodicTime";
        protected const string PoolDuetimeKey = "poolDueTime";
        protected const string PoolPeriodictimeKey = "poolPeriodicTime";
        protected const string PoolMinimumClientsToKeepKey = "minimumClientsToKeepInPool";
        protected const string PoolMaximumClientsToSupportKey = "maximumClientsToSupportInPool";
        protected const string PoolMagicNumberKey = "magicNumber";
        protected const string PoolMaximumRetriesToPollClient = "maximumRetriesToPollClient";


        public virtual ICluster Build(CassandraClusterElement clusterConfig)
        {
            ICluster cluster = null;

            switch (clusterConfig.ClusterTypeEnum)
            {
                case ClusterType.Thrift:
                    cluster = this.BuilDefaultCluster(clusterConfig);
                    break;
                case ClusterType.Cql:
                    clusterConfig.PoolTypeEnum = PoolType.CqlPool;
                    cluster = this.BuilDefaultCluster(clusterConfig);
                    break;
                default:
                    throw new NotImplementedException(string.Format("ClusterType '{0}' not implemented.", clusterConfig.ClusterTypeEnum));
            }

            return cluster;
        }

        protected virtual ICluster BuilDefaultCluster(CassandraClusterElement clusterConfig)
        {
            var clusterFactory = new DefaultClusterFactory { FriendlyName = clusterConfig.FriendlyName };
            clusterFactory.PoolManager = this.BuildPoolManager(clusterConfig, clusterFactory.FriendlyName);
            return clusterFactory.Create();
        }

        protected virtual IClientPool BuildPoolManager(CassandraClusterElement clusterConfig, string clusterName)
        {
            IClientPool clientPool;
            switch (clusterConfig.PoolTypeEnum)
            {
                case PoolType.NoPool:
                    clientPool = this.buildNoClientPool(clusterConfig, clusterName);
                    break;
                case PoolType.SizeControlledPool:
                    clientPool = this.buildSizeControlledClientPool(clusterConfig, clusterName);
                    break;
                case PoolType.SizedKeyspacePool:
                    clientPool = this.BuildSizeKeyspaceControlledClientPool(clusterConfig, clusterName);
                    break;
                    case PoolType.CqlPool:
                    clientPool = this.BuildCqlClientPool(clusterConfig, clusterName);
                    break;
                default:
                    throw new NotImplementedException(string.Format("PoolType '{0}' not implemented.", clusterConfig.PoolTypeEnum));
            }

            return clientPool;
        }

        protected IClientPool BuildCqlClientPool(CassandraClusterElement clusterConfig, string clusterName)
        {
            int intTempValue = 0;
            var poolFactory = new SizeControlledClientPoolFactory<CqlDefaultClientPool>();
            poolFactory.Name = string.Concat(clusterName, "_CQLPool");
            poolFactory.ClientFactory = new CqlTransportFactory();
            poolFactory.EndpointManager = this.BuildEndpointManager(clusterConfig, poolFactory.Name);

            SpecialConnectionParameterElement specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolPeriodictimeKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.PeriodicTime = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMagicNumberKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MagicNumber = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumClientsToSupportKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumClientsToSupport = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumRetriesToPollClient);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumRetriesToPollClient = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMinimumClientsToKeepKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MinimumClientsToKeep = intTempValue;
            }

            return poolFactory.Create();
        }

        private IClientPool buildSizeControlledClientPool(CassandraClusterElement clusterConfig, string clusterName)
        {
            int intTempValue = 0;
            var poolFactory = new SizeControlledClientPoolFactory<SizeControlledClientPool>();
            poolFactory.Name = string.Concat(clusterName, "_sizeControlledPool");
            poolFactory.ClientFactory = this.buildClientFactory(clusterConfig);
            poolFactory.EndpointManager = this.BuildEndpointManager(clusterConfig, poolFactory.Name);

            SpecialConnectionParameterElement specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolPeriodictimeKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.PeriodicTime = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMagicNumberKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MagicNumber = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumClientsToSupportKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumClientsToSupport = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumRetriesToPollClient);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumRetriesToPollClient = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMinimumClientsToKeepKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MinimumClientsToKeep = intTempValue;
            }

            return poolFactory.Create();
        }

        private IClientPool BuildSizeKeyspaceControlledClientPool(CassandraClusterElement clusterConfig, string clusterName)
        {
            int intTempValue = 0;
            var poolFactory = new SizeControlledClientPoolFactory<SizeKeyspaceControlledClientPool>();
            poolFactory.Name = string.Concat(clusterName, "_sizeKeyspaceControlledPool");
            poolFactory.ClientFactory = this.buildClientFactory(clusterConfig);
            poolFactory.EndpointManager = this.BuildEndpointManager(clusterConfig, poolFactory.Name);

            SpecialConnectionParameterElement specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolPeriodictimeKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.PeriodicTime = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMagicNumberKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MagicNumber = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumClientsToSupportKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumClientsToSupport = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMaximumRetriesToPollClient);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MaximumRetriesToPollClient = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, PoolMinimumClientsToKeepKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                poolFactory.MinimumClientsToKeep = intTempValue;
            }

            return poolFactory.Create();
        }

        protected virtual IEndpointManager BuildEndpointManager(CassandraClusterElement clusterConfig, string poolName)
        {
            IEndpointManager endpointManager;
            var endpointManagerType = (EndpointManagerType)Enum.Parse(typeof(EndpointManagerType), clusterConfig.EndpointManager.Type, true);
            switch (endpointManagerType)
            {
                case EndpointManagerType.RoundRobin:
                    endpointManager = this.buildRoundRobinEndpointManager(clusterConfig, poolName);
                    break;
                default:
                    throw new NotImplementedException(string.Format("EndpointManagerType '{0}' not implemented.", endpointManagerType));
            }

            return endpointManager;
        }

        /// <summary>
        /// Retrieves a list of endpoints used by the cluster
        /// </summary>
        /// <param name="endpointManager">the cluster endpoint manager</param>
        /// <returns>cluster endpoints</returns>
        private CassandraEndpointCollection GetEndpointCollection(EndpointManagerElement endpointManager)
        {
            // manual endpoints
            if (string.IsNullOrEmpty(endpointManager.Factory))
            {
                return endpointManager.CassandraEndpoints;
            }

            var factory = FactoryExtensions.GetEndpointFactory(endpointManager.Factory);
            if (factory == null)
            {
                throw new AquilesConfigurationException("Enpoint factory '" + endpointManager.Factory + "' not set");
            }

            var endpoints = factory(endpointManager.FactorySource);
            if (endpoints == null)
            {
                throw new AquilesConfigurationException("Error getting endpoints from factory '" + endpointManager.Factory + "'");
            }

            var collection = new CassandraEndpointCollection();
            var i = 0;
            foreach (var endpoint in endpoints)
            {
                ++i;
                collection.Add(endpoint);
            }

            return i == 0
                ? endpointManager.CassandraEndpoints
                : collection;
        }

        private IEndpointManager buildRoundRobinEndpointManager(CassandraClusterElement clusterConfig, string poolName)
        {
            var endpointManagerFactory = new RoundRobinEndpointManagerFactory();
            endpointManagerFactory.Name = string.Concat(poolName, "_endpointManager");
            endpointManagerFactory.ClientFactory = this.buildClientFactory(clusterConfig);

            var endpoints = this.GetEndpointCollection(clusterConfig.EndpointManager);
            endpointManagerFactory.Endpoints = this.buildEndpoints(endpoints, clusterConfig.EndpointManager.DefaultTimeout);
            SpecialConnectionParameterElement specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, EndpointmanagerPeriodictimeKey);
            int intTempValue = 0;
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                endpointManagerFactory.DueTime = intTempValue;
            }

            specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, EndpointmanagerPeriodictimeKey);
            if (specialConfig != null && int.TryParse(specialConfig.Value, out intTempValue))
            {
                endpointManagerFactory.PeriodicTime = intTempValue;
            }

            return endpointManagerFactory.Create();
        }

        protected virtual List<IEndpoint> buildEndpoints(CassandraEndpointCollection cassandraEndpointCollection, int defaultTimeout)
        {
            return (
                from CassandraEndpointElement endpointConfig in cassandraEndpointCollection
                select this.buildEndpoint(endpointConfig, defaultTimeout)).ToList();
        }

        protected abstract IConnectionFactory buildClientFactory(CassandraClusterElement clusterConfig);

        protected virtual IEndpoint buildEndpoint(CassandraEndpointElement endpointConfig, int defaultTimeout)
        {
            DefaultEndpoint endpoint = new DefaultEndpoint();
            endpoint.Address = endpointConfig.Address;
            endpoint.Port = endpointConfig.Port;
            endpoint.Timeout = (endpointConfig.Timeout != 0) ? endpointConfig.Timeout : defaultTimeout;

            return endpoint;
        }

        private IClientPool buildNoClientPool(CassandraClusterElement clusterConfig, string clusterName)
        {
            NoClientPoolFactory poolFactory = new NoClientPoolFactory();
            poolFactory.Name = string.Concat(clusterName, "_noPool");
            poolFactory.ClientFactory = this.buildClientFactory(clusterConfig);
            poolFactory.EndpointManager = this.BuildEndpointManager(clusterConfig, poolFactory.Name);
            //poolFactory.Logger = logger;

            return poolFactory.Create();
        }

        protected static SpecialConnectionParameterElement retrieveSpecialParameter(SpecialConnectionParameterCollection specialConnectionParameterCollection, string propertyKey)
        {
            SpecialConnectionParameterElement element = null;
            if (specialConnectionParameterCollection != null)
            {
                element = specialConnectionParameterCollection[propertyKey];
            }

            return element;
        }
    }
}
