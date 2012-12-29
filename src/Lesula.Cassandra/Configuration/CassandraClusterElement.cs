// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CassandraClusterElement.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   ConfigurationElement holding Cluster information
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System.Configuration;

    /// <summary>
    /// ConfigurationElement holding Cluster information
    /// </summary>
    public class CassandraClusterElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the cluster type enum.
        /// </summary>
        public AbstractAquilesClusterBuilder.ClusterType ClusterTypeEnum { get; set; }

        /// <summary>
        /// Gets or sets the pool type enum.
        /// </summary>
        public AbstractAquilesClusterBuilder.PoolType PoolTypeEnum { get; set; }

        /// <summary>
        /// get or set the Friendly name
        /// </summary>
        [ConfigurationProperty("friendlyName", DefaultValue = " ", IsRequired = true)]
        [StringValidator(MinLength = 1, MaxLength = int.MaxValue)]
        public string FriendlyName
        {
            get { return (string)this["friendlyName"]; }
            set { this["friendlyName"] = value; }
        }

        /// <summary>
        /// How many times the client should a command after a recoverable error ?
        /// </summary>
        [ConfigurationProperty("maximumRetries", DefaultValue = 0, IsRequired = false)]
        public int MaximumRetries
        {
            get { return (int)this["maximumRetries"]; }
            set { this["maximumRetries"] = value; }
        }

        /// <summary>
        /// get or set the Friendly name
        /// </summary>
        [ConfigurationProperty("clusterType", DefaultValue = "THRIFT", IsRequired = false)]
        [StringValidator(MinLength = 1, MaxLength = int.MaxValue)]
        public string ClusterType
        {
            get { return (string)this["clusterType"]; }
            set { this["clusterType"] = value; }
        }

        /// <summary>
        /// get or set the Connection configuration
        /// </summary>
        [ConfigurationProperty("connection", IsRequired = false)]
        public ConnectionElement Connection
        {
            get
            {
                return (ConnectionElement)this["connection"];
            }

            set
            {
                this["connection"] = value;
            }
        }

        /// <summary>
        /// get or set the endpoint manager configuration
        /// </summary>
        [ConfigurationProperty("endpointManager", IsRequired = true)]
        public EndpointManagerElement EndpointManager
        {
            get
            {
                return (EndpointManagerElement)this["endpointManager"];
            }

            set
            {
                this["endpointManager"] = value;
            }
        }
    }
}
