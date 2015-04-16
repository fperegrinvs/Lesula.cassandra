// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ICluster.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the ICluster type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Cluster
{
    using Lesula.Cassandra;
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Configuration;
    using Lesula.Cassandra.Model;
    using System.Collections.Generic;

    public interface ICluster
    {
        string Name
        {
            get;
        }

        /// <summary>
        /// How many times the client should a command after a recoverable error ?
        /// </summary>
        int MaximumRetries { get; set; }

        IClient Borrow();
        IClient Borrow(string keyspaceName);
        void Release(IClient client);
        void Invalidate(IClient client);
        T Execute<T>(ExecutionBlock<T> command);
        T Execute<T>(ExecutionBlock<T> command, string keyspaceName);

        List<IEndpoint> EndPoints { get;  }
    }
}
