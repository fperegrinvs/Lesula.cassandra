using System;
using System.Collections.Generic;
using System.Text;
using Lesula.Cassandra.Model;
using Lesula.Cassandra.Connection.Pooling;

namespace Lesula.Cassandra.Connection.Factory
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public interface IConnectionFactory
    {
        IClient Create(IEndpoint endpoint, IClientPool ownerPool);
    }
}
