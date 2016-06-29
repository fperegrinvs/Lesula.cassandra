using System;
using Apache.Cassandra;

namespace Lesula.Cassandra.FrontEnd
{
    public interface IRowDeletor
    {
        void DeleteRow(string columnFamily, string rowKey, ConsistencyLevel consistencyLevel);
        void DeleteRow(string columnFamily, Guid rowKey, ConsistencyLevel consistencyLevel);
        void DeleteRow(string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel);
    }
}