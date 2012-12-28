using System;
using System.Collections.Generic;
using System.Text;

namespace Lesula.Cassandra.Model
{
    public interface IEndpoint
    {
        string Address
        {
            get;
        }

        int Port
        {
            get;
        }

        int Timeout
        {
            get;
        }
    }

}
