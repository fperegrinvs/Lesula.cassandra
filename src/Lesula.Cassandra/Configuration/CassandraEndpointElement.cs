namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;

    /// <summary>
    /// Configuration Element to hold cassandra endpoint information
    /// </summary>
    public class CassandraEndpointElement : ConfigurationElement
    {
        /// <summary>
        /// get or set the address
        /// </summary>
        [ConfigurationProperty("address", DefaultValue = "localhost", IsRequired = true)]
        [StringValidator(InvalidCharacters = "~!@#$%^&*()[]{}/;'\"|\\:,", MinLength = 1, MaxLength = int.MaxValue)]
        public string Address
        {
            get { return (string)this["address"]; }
            set { this["address"] = value; }
        }

        /// <summary>
        /// get or set the Port
        /// </summary>
        [ConfigurationProperty("port", DefaultValue = "9160", IsRequired = true)]
        [IntegerValidator(MinValue = 1, MaxValue = Int16.MaxValue)]
        public int Port
        {
            get { return (int)this["port"]; }
            set { this["port"] = value; }
        }

        /// <summary>
        /// get or set the timeout
        /// </summary>
        [ConfigurationProperty("timeout", DefaultValue = "0", IsRequired = false)]
        [IntegerValidator(MinValue = 0, MaxValue = Int32.MaxValue)]
        public int Timeout
        {
            get
            {
                return (int)this["timeout"];
            }
            set
            {
                this["timeout"] = value;
            }
        }
    }
}
