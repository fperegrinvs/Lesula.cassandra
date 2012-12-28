// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DefaultClusterFactory.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the DefaultClusterFactory type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Cluster.Factory
{
    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster.Impl;
    using Lesula.Cassandra.Connection.Pooling;

    /// <summary>
    /// The default cluster factory.
    /// </summary>
    public class DefaultClusterFactory : IFactory<ICluster>
    {
        /// <summary>
        /// Gets or sets the pool manager.
        /// </summary>
        public IClientPool PoolManager { get; set; }

        /// <summary>
        /// Gets or sets the friendly name.
        /// </summary>
        public string FriendlyName { get; set; }

        /// <summary>
        /// How many times the client should a command after a recoverable error ?
        /// </summary>
        public int MaximumRetries { get; set; }

        #region IFactory<ICluster> Members

        public ICluster Create()
        {
            var cluster = new DefaultCluster
                {
                    PoolManager = this.PoolManager,
                    Name = this.FriendlyName,
                    MaximumRetries = this.MaximumRetries
                };

            return cluster;
        }

        #endregion
    }
}
