using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lesula.Cassandra.Connection.Pooling
{
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;

    public interface ISizeControlledPool : IClientPool
    {
        int MinimumClientsToKeep { get; set; }

        int MaximumClientsToSupport { get; set; }

        int MagicNumber { get; set; }

        int MaximumRetriesToPollClient { get; set; }

        int DueTime { get; set; }

        int PeriodicTime { get; set; }

        IEndpointManager EndpointManager { get; set; }

        IConnectionFactory ClientFactory { get; set; }

        void Initialize();
    }
}
