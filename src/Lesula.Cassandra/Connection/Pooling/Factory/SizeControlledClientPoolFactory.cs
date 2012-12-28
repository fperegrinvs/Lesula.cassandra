namespace Lesula.Cassandra.Connection.Pooling.Factory
{
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Connection.Pooling.Impl;

    public class SizeKespaceControlledClientPoolFactory : IFactory<SizeControlledClientPool>
    {
        private const int Defaultperiodicttime = 5000; // 5 sec
        private const int DefaultMinimumClientsToKeep = 10;
        private const int DefaultMaximumClientsToSupport = 100;
        private const int DefaultMagicNumber = 7;
        private const int DefaultMaximumRetriesToPollClient = 0;

        public SizeKespaceControlledClientPoolFactory()
        {
            this.MinimumClientsToKeep = DefaultMinimumClientsToKeep;
            this.MaximumClientsToSupport = DefaultMaximumClientsToSupport;
            this.MagicNumber = DefaultMagicNumber;
            this.MaximumRetriesToPollClient = DefaultMaximumRetriesToPollClient;
            this.PeriodicTime = Defaultperiodicttime;
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

        public SizeControlledClientPool Create()
        {
            SizeControlledClientPool pool = new SizeControlledClientPool();
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
