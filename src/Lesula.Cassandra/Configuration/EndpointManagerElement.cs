// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EndpointManagerElement.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Configuration element to hold endpointManager information
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;

    /// <summary>
    /// Configuration element to hold endpointManager information
    /// </summary>
    public class EndpointManagerElement : ConfigurationElement
    {
        /// <summary>
        /// get or set the type to use
        /// </summary>
        [ConfigurationProperty("type", DefaultValue = "ROUNDROBIN", IsRequired = true)]
        public string Type
        {
            get
            {
                return (string)this["type"];
            }

            set
            {
                this["type"] = value;
            }
        }

        /// <summary>
        /// get or set the factory to use
        /// </summary>
        [ConfigurationProperty("factory", IsRequired = false)]
        public string Factory
        {
            get
            {
                return (string)this["factory"];
            }

            set
            {
                this["factory"] = value;
            }
        }

        /// <summary>
        /// get or set the source to use with the factory
        /// </summary>
        [ConfigurationProperty("source", IsRequired = false)]
        public string FactorySource
        {
            get
            {
                return (string)this["source"];
            }

            set
            {
                this["source"] = value;
            }
        }

        /// <summary>
        /// get or set the collection of CassandraEndpoints
        /// </summary>
        [ConfigurationProperty("cassandraEndpoints", IsRequired = true)]
        [ConfigurationCollection(typeof(CassandraClusterElement), AddItemName = "add", RemoveItemName = "remove", ClearItemsName = "clear")]
        public CassandraEndpointCollection CassandraEndpoints
        {
            get
            {
                return (CassandraEndpointCollection)this["cassandraEndpoints"];
            }

            set
            {
                this["cassandraEndpoints"] = value;
            }
        }

        /// <summary>
        /// get or set the DefaultTime to be used when the CassandraEndpoint does not explicity declare one.
        /// </summary>
        [ConfigurationProperty("defaultTimeout", DefaultValue = "3000", IsRequired = true)]
        [IntegerValidator(MinValue = 1, MaxValue = Int32.MaxValue)]
        public int DefaultTimeout
        {
            get
            {
                return (int)this["defaultTimeout"];
            }

            set
            {
                this["defaultTimeout"] = value;
            }
        }
    }
}
