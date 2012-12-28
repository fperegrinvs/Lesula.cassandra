using System.Collections.Generic;

namespace Lesula.Cassandra.Connection.EndpointManager
{
    using Lesula.Cassandra.Model;

    public interface IEndpointManager
    {
        List<IEndpoint> Endpoints
        {
            get;
        }

        IEndpoint Pick();
        void Ban(IEndpoint endpoint);

    }
}
