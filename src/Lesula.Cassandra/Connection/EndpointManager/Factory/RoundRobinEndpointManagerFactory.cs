namespace Lesula.Cassandra.Connection.EndpointManager.Factory
{
    using System.Collections.Generic;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Connection.EndpointManager.Impl;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Model;

    public class RoundRobinEndpointManagerFactory : IFactory<RoundRobinEndpointManager>
    {
        private const int DEFAULTDUETIME = 5000; // 5 sec
        private const int DEFAULTPERIODICTTIME = 60000; // 1 min

        public string Name
        {
            get;
            set;
        }

        public int DueTime
        {
            get;
            set;
        }

        public int PeriodicTime
        {
            get;
            set;
        }

        public IConnectionFactory ClientFactory
        {
            set;
            get;
        }

        public List<IEndpoint> Endpoints
        {
            get;
            set;
        }

        public RoundRobinEndpointManagerFactory()
        {
            this.PeriodicTime = DEFAULTPERIODICTTIME;
            this.DueTime = DEFAULTDUETIME;
        }

        #region IFactory<RoundRobinEndpointManager> Members

        public RoundRobinEndpointManager Create()
        {
            RoundRobinEndpointManager endpointManager = new RoundRobinEndpointManager();
            endpointManager.ClientFactory = this.ClientFactory;
            endpointManager.DueTime = this.DueTime;
            endpointManager.PeriodicTime = this.PeriodicTime;
            endpointManager.Endpoints = this.Endpoints;
            return endpointManager;
        }

        #endregion
    }
}
