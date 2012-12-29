﻿namespace Lesula.Cassandra.Connection.Pooling.Impl
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Threading;

    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Client.Cql;
    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Exceptions;

    /// <summary>
    /// The default client pool for CQL binary protocol
    /// </summary>
    /// <remarks>
    /// As CQL support multiple calls for each connection, each client does not represent a distinct connection but an stream inside a connection
    /// </remarks>
    public class CqlDefaultClientPool : ISizeControlledPool
    {
        private volatile int managedClientQuantity;
        private readonly ConcurrentBag<IClient> idleClients;
        private readonly ConcurrentDictionary<IClient, byte> referencedClients;
        private int dueTime = Timeout.Infinite;
        private int periodicTime = Timeout.Infinite;
        private readonly Timer controlIdleClientSizeTimer;

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

        public int DueTime
        {
            get
            {
                return this.dueTime;
            }

            set
            {
                this.dueTime = value;
                this.controlIdleClientSizeTimer.Change(this.dueTime, this.periodicTime);
            }
        }

        public int PeriodicTime
        {
            get
            {
                return this.periodicTime;
            }

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

        public CqlDefaultClientPool()
        {
            this.managedClientQuantity = 0;
            this.referencedClients = new ConcurrentDictionary<IClient, byte>();
            this.idleClients = new ConcurrentBag<IClient>();
            this.controlIdleClientSizeTimer = new Timer(this.ControlIdleClientSizeMethod, null, Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary>
        /// Create the initial pool
        /// </summary>
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
                borrowedClient = this.PollClient();
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
                        this.Destroy(borrowedClient);
                        borrowedClient = null;
                    }
                }
                if (borrowedClient != null)
                {
                    // i got it, gonna mark as referenced 
                    this.MarkAsReferenced(borrowedClient);
                }

                retryCount++;
            }
            while ((retryCount < this.MaximumRetriesToPollClient) && (borrowedClient == null));

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
            this.Destroy(client);
        }
        #endregion

        private void Destroy(IClient client)
        {
            client.Close();
            this.managedClientQuantity--;
        }

        private void MarkAsReferenced(IClient client)
        {
            this.referencedClients.TryAdd(client, byte.MinValue);
        }

        private void MarkAsIdle(IClient client)
        {
            this.idleClients.Add(client);
        }

        private void UnmarkAsReferenced(IClient client)
        {
            byte output;
            this.referencedClients.TryRemove(client, out output);
        }

        private IClient PollClient()
        {
            IClient borrowedClient = null;
            this.idleClients.TryTake(out borrowedClient);
            return borrowedClient;
        }

        private IClient CreateNewClient()
        {
            this.managedClientQuantity++;
            IClient borrowedClient;

            do
            {
                var endpoint = this.EndpointManager.Pick();
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
            while (borrowedClient == null);

            return borrowedClient;
        }

        private void ControlIdleClientSizeMethod(object state)
        {
            // need to stop the time to avoid 2 methods run concurrent
            this.StopTimer();
            // i took half of the difference in order not to block for a long time the queue
            int difference = (this.MaximumClientsToSupport - this.MinimumClientsToKeep) / this.MagicNumber;
            try
            {
                if (this.idleClients.Count > this.MinimumClientsToKeep)
                {
                    HashSet<IClient> clientsToDestroy = new HashSet<IClient>();

                    for (int i = 0; (i < difference) && (this.idleClients.Count > this.MinimumClientsToKeep); i++)
                    {
                        IClient clientToDestroy;
                        if (this.idleClients.TryTake(out clientToDestroy))
                        {
                            clientsToDestroy.Add(clientToDestroy);
                        }
                    }

                    HashSet<IClient>.Enumerator destroyerIterator = clientsToDestroy.GetEnumerator();
                    while (destroyerIterator.MoveNext())
                    {
                        this.Destroy(destroyerIterator.Current);
                    }
                }
            }
            finally
            {
                this.StartTimer(this.periodicTime);
            }
        }

        private void CreateMinimumClients()
        {
            for (int i = 0; i < this.MinimumClientsToKeep; i++)
            {
                var client = this.CreateNewClient();
                if (client != null)
                {
                    this.idleClients.Add(client);
                }
                else
                {
                    AquilesHelper.Reset();
                    throw new AquilesException("No client could be created during startup phase.");
                }
            }
        }

        private void StopTimer()
        {
            this.controlIdleClientSizeTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        private void StartTimer(int duetime)
        {
            this.controlIdleClientSizeTimer.Change(duetime, Timeout.Infinite);
        }
    }
}
