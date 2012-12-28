// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CassandraConfigurationSection.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   ConfigurationSection for Aquiles
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;
    using System.Globalization;

    /// <summary>
    /// ConfigurationSection for Aquiles
    /// </summary>
    public class CassandraConfigurationSection : ConfigurationSection
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CassandraConfigurationSection"/> class. 
        /// </summary>
        protected CassandraConfigurationSection()
        {
        }

        /// <summary>
        /// Delay between a node failure and a cluster rebuild 
        /// (a imediate rebuild is also issued to clean the dead connections)
        /// </summary>
        [ConfigurationProperty("rebuildDelay", IsRequired = false, DefaultValue = "0")]
        public int RebuildDelay
        {
            get
            {
                return Convert.ToInt32(this["rebuildDelay"]);
            }

            set
            {
                this["rebuildDelay"] = value.ToString(CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// get or set the collection of clusters
        /// </summary>
        [ConfigurationProperty("clusters", IsRequired = true)]
        [ConfigurationCollection(typeof(CassandraClusterElement), AddItemName = "add", RemoveItemName = "remove", ClearItemsName = "clear")]
        public CassandraClusterCollection CassandraClusters
        {
            get
            {
                return (CassandraClusterCollection)this["clusters"];
            }

            set
            {
                this["clusters"] = value;
            }
        }
    }
}
