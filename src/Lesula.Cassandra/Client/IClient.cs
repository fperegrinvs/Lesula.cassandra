namespace Lesula.Cassandra.Client
{
    using System;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public interface IClient : IDisposable
    {
        IClientPool OwnerPool
        {
            get;
        }

        string KeyspaceName
        {
            get;
            set;
        }

        IEndpoint Endpoint
        {
            get;
        }

        void Open();
        void Close();
        bool IsOpen();
        T Execute<T>(ExecutionBlock<T> executionBlock);
        string getClusterName();
    }
}
