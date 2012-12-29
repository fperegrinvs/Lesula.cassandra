// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CqlTransportFactory.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the CqlTransportFactory type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Connection.Factory
{
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Client.CQL;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public class CqlTransportFactory : IConnectionFactory
    {
        #region Implementation of IConnectionFactory

        public IClient Create(IEndpoint endpoint, IClientPool ownerPool)
        {
            var config = new CqlConfig { Type = "CqlBinary", Recoverable = true, CqlVersion = "3.0.0", Streaming = true, };
            return new CqlClient(endpoint, ownerPool, config);
        }

        #endregion
    }
}
