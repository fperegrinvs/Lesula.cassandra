// --------------------------------------------------------------------------------------------------------------------
// <copyright file="AquilesHelper.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the AquilesHelper type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;

    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.Configuration;
    using Lesula.Cassandra.Exceptions;

    /// <summary>
    /// The aquiles helper.
    /// </summary>
    public sealed class AquilesHelper
    {
        /// <summary>
        /// The section configuration name.
        /// </summary>
        private const string SectionConfigurationName = "CassandraConfiguration";

        #region static

        /// <summary>
        /// Singleton for the AquilesInstance
        /// </summary>
        private static AquilesHelper instance;

        /// <summary>
        /// Initializes static members of the <see cref="AquilesHelper"/> class.
        /// </summary>
        static AquilesHelper()
        {
            var builder = new AquilesClusterBuilder();
            instance = new AquilesHelper(builder, SectionConfigurationName);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AquilesHelper"/> class.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="sectionConfigurationName">
        /// The section configuration name.
        /// </param>
        public AquilesHelper(AbstractAquilesClusterBuilder builder, string sectionConfigurationName)
        {
            this.BuildClusters(builder, sectionConfigurationName);
        }

        /// <summary>
        /// Delay between a node failure and a cluster rebuild 
        /// (a imediate rebuild is also issued to clean the dead connections)
        /// </summary>
        public static int RebuildDelay { get; set; }

        /// <summary>
        /// The cassandra clusters
        /// </summary>
        public Dictionary<string, ICluster> Clusters { get; set; }

        /// <summary>
        /// Reinicializa o cluster do Cassandra
        /// </summary>
        /// <param name="triggerDelayed">
        /// Indicates if this reset should trigger a delayed one
        /// </param>
        public static void Reset(bool triggerDelayed = true)
        {
            var builder = new AquilesClusterBuilder();
            instance.BuildClusters(builder, SectionConfigurationName);

            if (triggerDelayed && RebuildDelay > 0)
            {
                var thread = new Thread(DelayedReset);
                thread.Start(RebuildDelay);
            }
        }

        /// <summary>
        /// Read the configuration section, create logger, create clusters
        /// <remarks>can throw <see cref="AquilesException"/> in case something went wrong</remarks>
        /// </summary>
        public static void Initialize()
        {
            // do nothing, this is a trick for user clients
        }

        /// <summary>
        /// Retrieve a ICluster instance to work with.
        /// <remarks>can throw <see cref="AquilesException"/> in case something went wrong</remarks>
        /// </summary>
        /// <param name="clusterName">friendly names chosen in the configuration section on the .config file</param>
        /// <returns>it returns a cluster instance.</returns>
        public static ICluster RetrieveCluster(string clusterName)
        {
            if (string.IsNullOrEmpty(clusterName))
            {
                throw new ArgumentException("clusterName cannot be null nor empty");
            }

            return instance.Clusters[clusterName];
        }
        #endregion

        /// <summary>
        /// Build all clusters
        /// </summary>
        /// <param name="builder">
        /// A instance of the clusterBuilder
        /// </param>
        /// <param name="section">
        /// The configuration session
        /// </param>
        /// <returns>
        /// Cluster dictionary
        /// </returns>
        public static Dictionary<string, ICluster> BuildClusters(AbstractAquilesClusterBuilder builder, CassandraConfigurationSection section)
        {
            Dictionary<string, ICluster> clusters;
            CassandraClusterCollection clusterCollection = section.CassandraClusters;
            if (clusterCollection != null && clusterCollection.Count > 0)
            {
                clusters = new Dictionary<string, ICluster>(clusterCollection.Count);
                foreach (CassandraClusterElement clusterConfig in section.CassandraClusters)
                {
                    ICluster cluster;
                    try
                    {
                        cluster = builder.Build(clusterConfig);
                    }
                    catch (Exception e)
                    {
                        throw new AquilesConfigurationException("Exception found while creating clusters. See internal exception details.", e);
                    }

                    if (cluster != null)
                    {
                        if (!clusters.ContainsKey(cluster.Name))
                        {
                            clusters.Add(cluster.Name, cluster);
                        }
                    }
                }
            }
            else
            {
                throw new AquilesConfigurationException("Aquiles Configuration does not have any cluster configured.");
            }

            return clusters;
        }

        /// <summary>
        /// Delayed reset method
        /// </summary>
        /// <param name="o">
        /// The time.
        /// </param>
        private static void DelayedReset(object o)
        {
            var time = (int)o;
            Thread.Sleep(time * 1000);
            Reset(false);
        }

        /// <summary>
        /// Build call clusters defined the configurations
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="sectionConfigurationName">
        /// The section configuration name.
        /// </param>
        private void BuildClusters(AbstractAquilesClusterBuilder builder, string sectionConfigurationName)
        {
            var section = (CassandraConfigurationSection)ConfigurationManager.GetSection(sectionConfigurationName);
            if (section != null)
            {
                this.Clusters = BuildClusters(builder, section);
                RebuildDelay = section.RebuildDelay;
            }
            else
            {
                throw new AquilesConfigurationException("Configuration Section not found for '" + sectionConfigurationName + "'");
            }
        }
    }
}
