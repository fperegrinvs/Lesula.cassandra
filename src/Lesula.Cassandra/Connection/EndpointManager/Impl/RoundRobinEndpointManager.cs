﻿namespace Lesula.Cassandra.Connection.EndpointManager.Impl
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;

    using Lesula.Cassandra.Connection.EndpointManager;
    using Lesula.Cassandra.Connection.Factory;
    using Lesula.Cassandra.Exceptions;
    using Lesula.Cassandra.Model;

    public class RoundRobinEndpointManager : IEndpointManager, IDisposable
    {

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

        private List<IEndpoint> endpoints;
        private ConcurrentBag<IEndpoint> availableEndpoints;
        private HashSet<IEndpoint> blackListedEndpoints;
        private Timer endpointRecoveryTimer;

        public RoundRobinEndpointManager()
        {
            this.blackListedEndpoints = new HashSet<IEndpoint>();
            this.endpointRecoveryTimer = new Timer(this.EndpointRecoveryMethod, null, Timeout.Infinite, Timeout.Infinite);
        }

        public IConnectionFactory ClientFactory { private get; set; }

        #region IEndpointManager Members

        public List<IEndpoint> Endpoints
        {
            get
            {
                return this.endpoints;
            }

            set
            {
                this.endpoints = value;
                this.availableEndpoints = new ConcurrentBag<IEndpoint>(this.endpoints);
            }
        }

        public IEndpoint Pick()
        {
            IEndpoint borrowedEndpoint;
            do
            {
                if (!this.availableEndpoints.TryTake(out borrowedEndpoint))
                {
                    var sb = new StringBuilder();
                    foreach (var endpoint in this.Endpoints)
                    {
                        if (sb.Length > 0)
                        {
                            sb.Append("','");
                        }

                        sb.Append(endpoint.ToString());
                    }

                    var error = string.Format("All endpoints ['{0}'] are blacklisted, cluster is down?", sb.ToString());
                    throw new AquilesException(error);
                }

                if (this.IsBlackListed(borrowedEndpoint))
                {
                    borrowedEndpoint = null;
                }
                else
                {
                    this.availableEndpoints.Add(borrowedEndpoint);
                }
            }
            while (borrowedEndpoint == null);

            return borrowedEndpoint;
        }

        public void Ban(IEndpoint endpoint)
        {
            lock (this.blackListedEndpoints)
            {
                if (!this.blackListedEndpoints.Contains(endpoint))
                {
                    this.blackListedEndpoints.Add(endpoint);
                }
            }
        }

        #endregion

        #region IDisposable Members
        private bool disposed = false;
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(Boolean forceDispose)
        {
            if (!this.disposed)
            {
                this.disposed = true;
                if (forceDispose)
                {
                    //DO NOTHING YET
                }
            }
        }

        ~RoundRobinEndpointManager()
        {
            this.Dispose(false);
        }

        #endregion

        private bool IsBlackListed(IEndpoint borrowedEndpoint)
        {
            bool isBlackListed = false;
            if (this.blackListedEndpoints.Count > 0)
            {
                lock (this.blackListedEndpoints)
                {
                    isBlackListed = this.blackListedEndpoints.Contains(borrowedEndpoint);
                    //TODO hacer esto afuera del lock y si isblacklisted = true
                    this.StartTimer(this.DueTime);
                }
            }

            return isBlackListed;
        }

        private void EndpointRecoveryMethod(object state)
        {
            bool wasProperlyFinished = false;
            bool areThereLeftBehind = false;

            // need to stop the time to avoid 2 methods run concurrent
            this.StopTimer();
            try
            {
                HashSet<IEndpoint> clonedBlackList = null;
                lock (this.blackListedEndpoints)
                {
                    clonedBlackList = new HashSet<IEndpoint>(this.blackListedEndpoints);
                }

                // now i can work without problems
                foreach (var endpoint in clonedBlackList)
                {
                    var cassandraClient = this.ClientFactory.Create(endpoint, null);

                    bool isUp;
                    try
                    {
                        cassandraClient.Open();
                        cassandraClient.getClusterName();
                        isUp = true;
                    }
                    catch
                    {
                        // this endpoint is still down :(
                        isUp = false;
                    }
                    finally
                    {
                        cassandraClient.Close();
                    }

                    if (isUp)
                    {
                        lock (this.blackListedEndpoints)
                        {
                            this.blackListedEndpoints.Remove(endpoint);
                        }

                        this.availableEndpoints.Add(endpoint);
                    }
                    else
                    {
                        areThereLeftBehind = true;
                    }
                }

                wasProperlyFinished = true;
            }
            catch (Exception)
            {
            }
            finally
            {
                if (wasProperlyFinished && areThereLeftBehind)
                {
                    // i should reactive myself again 
                    this.StartTimer(this.PeriodicTime);
                }
                else if (!wasProperlyFinished)
                {
                    // damn, i want my revenge!
                    this.StartTimer(this.DueTime);
                }
                else
                {
                    // i get to here, im done... im gonna go to the beach! :)
                }
            }
        }

        private void StopTimer()
        {
            this.endpointRecoveryTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        private void StartTimer(int dueTime)
        {
            this.endpointRecoveryTimer.Change(dueTime, Timeout.Infinite);
        }
    }
}
