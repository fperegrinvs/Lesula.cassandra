// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DefaultCluster.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   Defines the DefaultCluster type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Cluster.Impl
{
    using System;
    using System.Threading.Tasks;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Client.Cql;
    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Exceptions;

    public class DefaultCluster : ICluster
    {
        public IClientPool PoolManager
        {
            get;
            set;
        }

        /// <summary>
        /// How many times the client should a command after a recoverable error ?
        /// </summary>
        public int MaximumRetries { get; set; }

        #region ICluster Members

        public string Name { get; set; }


        public IClient Borrow()
        {
            return this.PoolManager.Borrow();
        }

        public string ExecuteNonQueryAsync(string cql, CqlConsistencyLevel cl)
        {
            var client = this.PoolManager.Borrow();
            return client.ExecuteNonQueryAsync(cql, cl);
        }

        public IClient Borrow(string keyspaceName)
        {
            IClient client = this.PoolManager.Borrow(keyspaceName);
            if (client != null && client.KeyspaceName != keyspaceName)
            {
                client.KeyspaceName = keyspaceName;
            }

            return client;
        }

        public void Release(IClient client)
        {
            this.PoolManager.Release(client);
        }

        public void Invalidate(IClient client)
        {
            this.PoolManager.Invalidate(client);
        }

        public T QueryAsync<T>(string cql, ICqlObjectBuilder<T> builder, CqlConsistencyLevel cl)
        {
            var client = this.PoolManager.Borrow();
            return client.QueryAsync(cql, builder, cl);
        }

        public T Execute<T>(ExecutionBlock<T> executionBlock, string keyspaceName)
        {
            T rtnObject = default(T);
            int executionCounter = 0;
            bool noException;
            Exception exception;
            do
            {
                exception = null;
                noException = false;
                bool isClientHealthy = true;
                var client = this.BorrowClient(keyspaceName);

                if (client == null)
                {
                    AquilesHelper.Reset();
                    throw new AquilesException("No client could be borrowed.");
                }

                try
                {
                    rtnObject = client.Execute(executionBlock);
                    noException = true;
                }
                catch (ExecutionBlockException ex)
                {
                    exception = ex;
                    isClientHealthy = ex.IsClientHealthy;
                    if (!ex.ShouldRetry)
                    {
                        executionCounter = 0;
                    }
                }
                finally
                {
                    if (noException || isClientHealthy)
                    {
                        this.Release(client);
                    }
                    else
                    {
                        this.Invalidate(client);
                    }
                }

                executionCounter++;
            }
            while (executionCounter < this.MaximumRetries && !noException);

            if (exception != null)
            {
                if (executionCounter == this.MaximumRetries)
                {
                    AquilesHelper.Reset();
                }

                throw exception;
            }

            return rtnObject;
        }

        public T Execute<T>(ExecutionBlock<T> executionBlock)
        {
            return this.Execute(executionBlock, null);
        }

        #endregion

        private IClient BorrowClient(string keyspaceName)
        {
            IClient client = null;
            if (keyspaceName != null)
            {
                client = this.Borrow(keyspaceName);
            }
            else
            {
                client = this.Borrow();
            }

            return client;
        }
    }
}
