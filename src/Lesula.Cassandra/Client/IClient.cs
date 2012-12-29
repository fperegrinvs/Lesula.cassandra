namespace Lesula.Cassandra.Client
{
    using System;
    using System.Threading.Tasks;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Client.Cql;
    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public interface IClient : IDisposable
    {
        IClientPool OwnerPool
        {
            get;
        }

        IEndpoint Endpoint
        {
            get;
        }

        void Open();
        void Close();
        bool IsOpen();
        string getClusterName();

        // Thrift
        string KeyspaceName { get; set; }
        T Execute<T>(ExecutionBlock<T> executionBlock);

        // CQL
        T QueryAsync<T>(string cql, ICqlObjectBuilder<T> builder, CqlConsistencyLevel cl);
        string ExecuteNonQueryAsync(string cql, CqlConsistencyLevel cl);
    }
}
