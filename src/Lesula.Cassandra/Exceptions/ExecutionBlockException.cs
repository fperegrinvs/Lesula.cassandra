using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lesula.Cassandra.Exceptions
{
    [Serializable]
    public class ExecutionBlockException : Exception
    {
        public bool ShouldRetry
        {
            get;
            set;
        }

        public bool IsClientHealthy
        {
            get;
            set;
        }

        /// <summary>
        /// ctor
        /// </summary>
        public ExecutionBlockException() : base() { }
        /// <summary>
        /// ctor
        /// </summary>
        public ExecutionBlockException(string message) : base(message) { }
        /// <summary>
        /// ctor
        /// </summary>
        public ExecutionBlockException(string message, Exception ex) : base(message, ex) { }
    }
}
