using System;
using System.Collections.Generic;

using System.Text;

namespace Lesula.Cassandra.Helpers
{
    public class UnixHelper
    {
        private UnixHelper() { }

        static readonly DateTime unitStartDateTime = new DateTime(1970, 1, 1).ToUniversalTime();

        public static long UnixTimestamp
        {
            get { return Convert.ToInt64((DateTime.UtcNow - unitStartDateTime).TotalMilliseconds); }
        }
    }
}
