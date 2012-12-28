// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyspaceManager.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Management operations need to be applied to a single node.
//   See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System.Collections.Generic;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster;

    /// <summary>
    /// Management operations need to be applied to a single node.
    /// See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
    /// </summary>
    public class KeyspaceManager
    {
        public const string KsdefStrategySimple = "org.apache.cassandra.locator.SimpleStrategy";
        public const string KsdefStrategyLocal = "org.apache.cassandra.locator.LocalStrategy";
        public const string KsdefStrategyNetworkTopology = "org.apache.cassandra.locator.NetworkTopologyStrategy";
        public const string KsdefStrategyNetworkTopologyOld = "org.apache.cassandra.locator.OldNetworkTopologyStrategy";

        /// <summary>
        /// The cluster.
        /// </summary>
        private readonly ICluster cluster;

        public KeyspaceManager(ICluster cluster)
        {
            this.cluster = cluster;
        }

        public List<KsDef> GetKeyspaceNames()
        {
            var operation = new ExecutionBlock<List<KsDef>>(myclient => myclient.describe_keyspaces());
            return this.cluster.Execute(operation);
        }

        public List<TokenRange> GetKeyspaceRingMappings(string keyspace)
        {
            var operation = new ExecutionBlock<List<TokenRange>>(myclient => myclient.describe_ring(keyspace));
            return this.cluster.Execute(operation);
        }

        public KsDef GetKeyspaceSchema(string keyspace)
        {
            var operation = new ExecutionBlock<KsDef>(myclient => myclient.describe_keyspace(keyspace));
            return this.cluster.Execute(operation);
        }

        public bool TryAddKeyspace(string name, int replicationFactor)
        {
            try
            {
                this.AddKeyspace(name, replicationFactor);
            }
            catch (InvalidRequestException)
            {
                return false;
            }

            return true;
        }

        public string AddKeyspace(string name, int replicationFactor)
        {
            var keyDefinition = new KsDef
                {
                    Name = name,
                    Replication_factor = replicationFactor,
                    Strategy_class = KsdefStrategySimple,
                    Cf_defs = new List<CfDef>(),
                    Strategy_options = new Dictionary<string, string>()
                };

            keyDefinition.Strategy_options["replication_factor"] = replicationFactor.ToString();

            return this.AddKeyspace(keyDefinition);
        }

        /// <summary>
        /// Atualiza configurações de um keyspace
        /// </summary>
        /// <param name="keyspaceDefinition">definições do keyspace</param>
        /// <returns>id do schema</returns>
        public string UpdateKeyspace(KsDef keyspaceDefinition)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_update_keyspace(keyspaceDefinition));
            return this.cluster.Execute(operation);
        }

        /// <summary>
        /// Descreve um keyspace
        /// </summary>
        /// <param name="name">nome do keyspace</param>
        /// <returns>Descrição do keyspace</returns>
        public KsDef DescribeKeyspace(string name)
        {
            var operation = new ExecutionBlock<KsDef>(myclient => myclient.describe_keyspace(name));
            return this.cluster.Execute(operation);
        }

        public string AddKeyspace(KsDef keyspaceDefinition)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_add_keyspace(keyspaceDefinition));
            return this.cluster.Execute(operation);
        }

        public string DropKeyspace(string keyspace)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_drop_keyspace(keyspace));
            return this.cluster.Execute(operation);
        }
    }
}
