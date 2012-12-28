namespace Lesula.Cassandra.Configuration
{
    using System;

    /// <summary>
    /// Exception thrown when there are invalid or missing values over the configuration
    /// </summary>
    public class AquilesConfigurationException : Exception
    {
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesConfigurationException() : base() { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesConfigurationException(string message) : base(message) { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesConfigurationException(string message, Exception ex) : base(message, ex) { }
    }
}
