using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;

namespace Lesula.Cassandra.Model
{
    public abstract class AbstractEndpoint : IEndpoint
    {
        #region IEndpoint Members

        public string Address
        {
            get;
            set;
        }

        public int Port
        {
            get;
            set;
        }

        public int Timeout
        {
            get;
            set;
        }

        #endregion


        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}:{1}-{2}", this.Address, this.Port, this.Timeout);
        }
    }
}
