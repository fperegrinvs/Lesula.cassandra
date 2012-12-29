// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CqlTransportFactory.cs" company="Lesula MapReduce Framework - http://github.com/lstern/Lesula.cassandra">
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
//   Defines the CqlTransportFactory type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Connection.Factory
{
    using System.Collections.Generic;

    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Client.Cql;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public class CqlTransportFactory : IConnectionFactory
    {
        private readonly Dictionary<string, List<CqlConnection>> connections;

        private readonly CqlConfig config;

        private Dictionary<string, object> connectionsLock;

        private object endpointsLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlTransportFactory"/> class.
        /// </summary>
        public CqlTransportFactory()
        {
            this.connections = new Dictionary<string, List<CqlConnection>>();
            this.connectionsLock = new Dictionary<string, object>();
            this.config = new CqlConfig { Type = "CqlBinary", Recoverable = true, CqlVersion = "3.0.0", Streaming = true };
        }

        #region Implementation of IConnectionFactory

        /// <remarks>
        /// When no ownerPool is defined, it's the EndpointManager testing if the endpoint is alive
        /// </remarks>
        public IClient Create(IEndpoint endpoint, IClientPool ownerPool)
        {
            if (!this.connections.ContainsKey(endpoint.Address))
            {
                lock (this.endpointsLock)
                {
                    if (!this.connections.ContainsKey(endpoint.Address))
                    {
                        this.connections[endpoint.Address] = new List<CqlConnection>();
                    }
                }
            }

            var connectionList = this.connections[endpoint.Address];
            CqlConnection connection;

            for (var i = 0; i < connectionList.Count; i++)
            {
                if (connectionList[i].HaveFreeSessions)
                {
                    connection = connectionList[i];
                    return connection.GetClient();
                }
            }

            if (!this.connectionsLock.ContainsKey(endpoint.Address))
            {
                this.connectionsLock[endpoint.Address] = new object();
            }

            var count = connectionList.Count;
            lock (this.connectionsLock[endpoint.Address])
            {
                if (connectionList.Count == count)
                {
                    connection = new CqlConnection(endpoint, this.config);
                    connectionList.Add(connection);
                }
                else
                {
                    // something changed, try again
                    return this.Create(endpoint, ownerPool);
                }
            }

            return connection.GetClient();
        }

        #endregion
    }
}
