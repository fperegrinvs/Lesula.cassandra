namespace Lesula.Cassandra.Connection.Pooling.Factory
{
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Connection.Pooling.Impl;

    public class SizeKeyspaceControlledClientPoolFactory : IFactory<SizeKeyspaceControlledClientPool>
    {
        private const int DEFAULTPERIODICTTIME = 5000; // 5 sec

        private const int DEFAULT_MINIMUM_CLIENTS_TO_KEEP = 10;
        private const int DEFAULT_MAXIMUM_CLIENTS_TO_SUPPORT = 1000;
        private const int DEFAULT_MAGIC_NUMBER = 7;
        private const int DEFAULT_MAXIMUM_RETRIES_TO_POLL_CLIENT = 0;

        public SizeKeyspaceControlledClientPoolFactory()
        {
            this.MinimumClientsToKeep = DEFAULT_MINIMUM_CLIENTS_TO_KEEP;
            this.MaximumClientsToSupport = DEFAULT_MAXIMUM_CLIENTS_TO_SUPPORT;
            this.MagicNumber = DEFAULT_MAGIC_NUMBER;
            this.MaximumRetriesToPollClient = DEFAULT_MAXIMUM_RETRIES_TO_POLL_CLIENT;
            this.PeriodicTime = DEFAULTPERIODICTTIME;
        }

        public string Name
        {
            get;
            set;
        }

        public int MinimumClientsToKeep
        {
            get;
            set;
        }

        public int MaximumClientsToSupport
        {
            get;
            set;
        }

        public int MagicNumber
        {
            get;
            set;
        }

        public int PeriodicTime
        {
            get;
            set;
        }

        public int MaximumRetriesToPollClient
        {
            get;
            set;
        }

        public IEndpointManager EndpointManager
        {
            get;
            set;
        }

        public IConnectionFactory ClientFactory
        {
            get;
            set;
        }

        #region IFactory<SizeControlledConnectionPool> Members

        public SizeKeyspaceControlledClientPool Create()
        {
            SizeKeyspaceControlledClientPool pool = new SizeKeyspaceControlledClientPool();
            pool.ClientFactory = this.ClientFactory;
            pool.EndpointManager = this.EndpointManager;
            pool.MagicNumber = this.MagicNumber;
            pool.MaximumClientsToSupport = this.MaximumClientsToSupport;
            pool.MaximumRetriesToPollClient = this.MaximumRetriesToPollClient;
            pool.MinimumClientsToKeep = this.MinimumClientsToKeep;
            pool.PeriodicTime = this.PeriodicTime;
            pool.Initialize();

            return pool;
        }

        #endregion
    }
}
