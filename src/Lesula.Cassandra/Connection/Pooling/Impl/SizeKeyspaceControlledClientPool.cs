namespace Lesula.Cassandra.Connection.Pooling.Impl
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Threading;

    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Exceptions;
    using Lesula.Cassandra.Model;

    public class SizeKeyspaceControlledClientPool : IClientPool
    {
        private const string system = "_sys";
        private volatile int managedClientQuantity;
        private ConcurrentDictionary<string, ConcurrentBag<IClient>> idleClients;
        private ConcurrentDictionary<IClient, byte> referencedClients;
        private int dueTime = Timeout.Infinite;
        private int periodicTime = Timeout.Infinite;
        private Timer controlIdleClientSizeTimer;

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
        public int MaximumRetriesToPollClient
        {
            get;
            set;
        }

        public List<IEndpoint> EndPoints
        {
            get
            {
                return this.EndpointManager.Endpoints;
            }
        }

        public int DueTime
        {
            get { return this.dueTime; }
            set
            {
                this.dueTime = value;
                this.controlIdleClientSizeTimer.Change(this.dueTime, this.periodicTime);
            }
        }
        public int PeriodicTime
        {
            get { return this.periodicTime; }
            set
            {
                this.periodicTime = value;
                this.controlIdleClientSizeTimer.Change(this.dueTime, this.periodicTime);
            }
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

        public SizeKeyspaceControlledClientPool()
        {
            this.managedClientQuantity = 0;
            this.referencedClients = new ConcurrentDictionary<IClient, byte>();
            this.idleClients = new ConcurrentDictionary<string, ConcurrentBag<IClient>>();
            this.controlIdleClientSizeTimer = new Timer(this.ControlIdleClientSizeMethod, null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Initialize()
        {
            this.CreateMinimumClients();
        }

        #region IConnectionPool Members


        public IClient Borrow(string keyspace = null)
        {
            IClient borrowedClient = null;
            int retryCount = 0;
            do
            {
                borrowedClient = this.PollClient(keyspace);
                if (borrowedClient == null)
                {
                    if (this.managedClientQuantity < this.MaximumClientsToSupport)
                    {
                        // i got no connection, need to retrieve a new one
                        borrowedClient = this.CreateNewClient();
                    }
                }
                else
                {
                    if (!borrowedClient.IsOpen())
                    {
                        this.destroy(borrowedClient);
                        borrowedClient = null;
                    }
                }
                if (borrowedClient != null)
                {
                    // i got it, gonna mark as referenced 
                    this.MarkAsReferenced(borrowedClient);
                }
                retryCount++;
            } while ((retryCount < this.MaximumRetriesToPollClient)
                && (borrowedClient == null));

            return borrowedClient;
        }

        public void Release(IClient client)
        {
            this.UnmarkAsReferenced(client);
            this.MarkAsIdle(client);
        }

        public void Invalidate(IClient client)
        {
            this.UnmarkAsReferenced(client);
            this.EndpointManager.Ban(client.Endpoint);
            this.destroy(client);
        }
        #endregion

        private void destroy(IClient client)
        {
            client.Close();
            this.managedClientQuantity--;
        }

        private void MarkAsReferenced(IClient client)
        {
            this.referencedClients.TryAdd(client, byte.MinValue);
        }

        private ConcurrentBag<IClient> CreateBag(string key)
        {
            return new ConcurrentBag<IClient>();
        }

        private void MarkAsIdle(IClient client)
        {
            ConcurrentBag<IClient> bag = this.idleClients.GetOrAdd(client.KeyspaceName ?? system, this.CreateBag);
            bag.Add(client);
        }

        private void UnmarkAsReferenced(IClient client)
        {
            byte output;
            this.referencedClients.TryRemove(client, out output);
        }

        private IClient PollClient(string keyspace)
        {
            IClient borrowedClient = null;
            ConcurrentBag<IClient> bag = this.idleClients.GetOrAdd(keyspace ?? system, this.CreateBag);
            bag.TryTake(out borrowedClient);
            return borrowedClient;
        }

        private IClient CreateNewClient()
        {
            this.managedClientQuantity++;
            IClient borrowedClient = null;
            IEndpoint endpoint = null;
            do
            {
                endpoint = this.EndpointManager.Pick();
                if (endpoint != null)
                {
                    borrowedClient = this.ClientFactory.Create(endpoint, this);
                    try
                    {
                        borrowedClient.Open();
                    }
                    catch (SocketException)
                    {
                        // ok this endpoint is not good
                        this.EndpointManager.Ban(endpoint);
                        borrowedClient = null;
                    }
                }
                else
                {
                    string message = string.Format("No endpoints available.");
                    throw new AquilesException(message);
                }
            }
            while (endpoint != null && borrowedClient == null);

            return borrowedClient;
        }

        private void ControlIdleClientSizeMethod(object state)
        {
            // need to stop the time to avoid 2 methods run concurrent
            this.stopTimer();

            // i took half of the difference in order not to block for a long time the queue
            int difference = (this.MaximumClientsToSupport - this.MinimumClientsToKeep) / this.MagicNumber;
            try
            {
                HashSet<IClient> clientsToDestroy = null;
                foreach (var keyspaceBag in this.idleClients.Values)
                {
                    if (this.idleClients.Count > this.MinimumClientsToKeep)
                    {
                        if (clientsToDestroy == null)
                        {
                            clientsToDestroy = new HashSet<IClient>();
                        }
                        for (int i = 0; (i < difference) && (keyspaceBag.Count > this.MinimumClientsToKeep); i++)
                        {
                            IClient clientToDestroy;
                            if (keyspaceBag.TryTake(out clientToDestroy))
                            {
                                clientsToDestroy.Add(clientToDestroy);
                            }
                        }
                    }
                }

                HashSet<IClient>.Enumerator destroyerIterator = clientsToDestroy.GetEnumerator();
                while (destroyerIterator.MoveNext())
                {
                    this.destroy(destroyerIterator.Current);
                }
            }
            finally
            {
                this.startTimer(this.periodicTime);
            }
        }

        private void CreateMinimumClients()
        {
            //for (int i = 0; i < this.MinimumClientsToKeep; i++)
            //{
            //    IClient client = this.CreateNewClient();
            //    if (client != null)
            //    {
            //        this.idleClients.Add(client);
            //    }
            //    else
            //    {
            //        throw new AquilesException("No client could be created during startup phase.");
            //    }
            //}
        }

        private void stopTimer()
        {
            this.controlIdleClientSizeTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        private void startTimer(int duetime)
        {
            this.controlIdleClientSizeTimer.Change(duetime, Timeout.Infinite);
        }
    }
}
