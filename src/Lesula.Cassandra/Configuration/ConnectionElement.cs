namespace Lesula.Cassandra.Configuration
{
    using System.Configuration;

    /// <summary>
    /// Configuration Element holding connection information
    /// </summary>
    public class ConnectionElement : ConfigurationElement
    {
        /// <summary>
        /// get or set pool Type
        /// </summary>
        [ConfigurationProperty("poolType", DefaultValue="NOPOOL", IsRequired = true)]
        public string PoolType
        {
            get
            {
                return (string) this["poolType"];
            }

            set
            {
                this["poolType"] = value;
            }
        }

        /// <summary>
        /// get or set factory type
        /// </summary>
        [ConfigurationProperty("factoryType", DefaultValue = "BUFFERED", IsRequired = true)]
        public string FactoryType
        {
            get
            {
                return (string)this["factoryType"];
            }
            set
            {
                this["factoryType"] = value;
            }
        }

        /// <summary>
        /// get or set collection of special parameters
        /// </summary>
        [ConfigurationProperty("specialConnectionParameters", IsRequired = false)]
        [ConfigurationCollection(typeof(SpecialConnectionParameterElement), AddItemName = "add", RemoveItemName = "remove", ClearItemsName = "clear")]
        public SpecialConnectionParameterCollection SpecialConnectionParameters
        {
            get
            {
                return (SpecialConnectionParameterCollection)this["specialConnectionParameters"];
            }
            set
            {
                this["specialConnectionParameters"] = value;
            }
        }

    }
}
