// --------------------------------------------------------------------------------------------------------------------
// <copyright file="AquilesClusterBuilder.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the AquilesClusterBuilder type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System;

    using Lesula.Cassandra.Connection.Factory;

    public sealed class AquilesClusterBuilder : AbstractAquilesClusterBuilder
    {
        /// <summary>
        /// Type of connection used to connect to Cassandra Cluster
        /// </summary>
        public enum ConnectionFactoryType
        {
            /// <summary>
            /// Default connection. It use binary protocol over socket transport
            /// </summary>
            DEFAULT = 0,

            /// <summary>
            /// Buffered connection. It use binary protocol over buffered transport (This is faster than Default)
            /// </summary>
            BUFFERED = 1,
            
            /// <summary>
            /// Framed connection. It use binary protocol over framed transport (if you are connecting to a nonblocking server (like Java's TNonblockingServer and THsHaServer) 
            /// </summary>
            FRAMED = 2,

            /// <summary>
            /// Fake connection, connect to a fake cassandra client, only used for tests
            /// </summary>
            FAKE = 3,
        }

        private const string CONNECTIONFACTORY_TRANSPORT_BUFFER_SIZE_OPTION = "transportBufferSize";

        protected override IConnectionFactory buildClientFactory(CassandraClusterElement clusterConfig)
        {
            IConnectionFactory connectionFactory = null;
            var connectionFactoryType = (ConnectionFactoryType)Enum.Parse(typeof(ConnectionFactoryType), clusterConfig.Connection.FactoryType, true);
            switch (connectionFactoryType)
            {
                case ConnectionFactoryType.BUFFERED:
                    connectionFactory = buildBufferedConnectionFactory(clusterConfig);
                    break;
                case ConnectionFactoryType.FRAMED:
                    connectionFactory = buildFramedConnectionFactory(clusterConfig);
                    break;
                case ConnectionFactoryType.DEFAULT:
                    connectionFactory = buildDefaultConnectionFactory(clusterConfig);
                    break;
                    case ConnectionFactoryType.FAKE:
                    connectionFactory = new FakeConnectionFactory();
                    break;
                default:
                    throw new NotImplementedException(string.Format("ConnectionFactoryType '{0}' not implemented.", connectionFactoryType));
            }

            return connectionFactory;
        }

        private static IConnectionFactory buildDefaultConnectionFactory(CassandraClusterElement clusterConfig)
        {
            var connectionFactory = new DefaultTransportConnectionFactory();
            return connectionFactory;
        }

        private static IConnectionFactory buildFramedConnectionFactory(CassandraClusterElement clusterConfig)
        {
            var connectionFactory = new FramedTransportConnectionFactory();
            return connectionFactory;
        }

        private static IConnectionFactory buildBufferedConnectionFactory(CassandraClusterElement clusterConfig)
        {
            BufferedTransportConnectionFactory connectionFactory = new BufferedTransportConnectionFactory();
            SpecialConnectionParameterElement specialConfig = retrieveSpecialParameter(clusterConfig.Connection.SpecialConnectionParameters, CONNECTIONFACTORY_TRANSPORT_BUFFER_SIZE_OPTION);
            int intTempValue = 0;
            if (specialConfig != null && Int32.TryParse(specialConfig.Value, out intTempValue))
            {
                connectionFactory.BufferSize = intTempValue;
            }

            return connectionFactory;
        }
    }
}
    