namespace Lesula.Cassandra.FrontEnd
{
    using System;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster;

    /// <summary>
    /// Facilitates the removal of data at a key-level.
    /// </summary>
    public class RowDeletor : IRowDeletor
    {
        /// <summary>
        /// The timestamp.
        /// </summary>
        private readonly long timestamp;

        /// <summary>
        /// The cluster.
        /// </summary>
        private readonly ICluster cluster;

        /// <summary>
        /// The keyspace.
        /// </summary>
        private readonly string keyspace;

        /// <summary>
        /// Initializes a new instance of the <see cref="RowDeletor"/> class.
        /// </summary>
        /// <param name="cluster">
        /// The cluster.
        /// </param>
        /// <param name="keyspace">
        /// The keyspace.
        /// </param>
        /// <remarks>
        /// -1 está sendo somado ao TimeStamp para "desempatar" com outros comandos do cassandra pois é possível apagar um registro antes de modifica-lo
        /// Mas não faz muito sentido apagar um registro logo após cria-lo.
        /// </remarks>
        public RowDeletor(ICluster cluster, string keyspace)
            : this(cluster, keyspace, DateTimeOffset.UtcNow.ToTimestamp() - 1)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RowDeletor"/> class.
        /// </summary>
        /// <param name="cluster">
        /// The cluster.
        /// </param>
        /// <param name="keyspace">
        /// The keyspace.
        /// </param>
        /// <param name="timestamp">
        /// The timestamp.
        /// </param>
        public RowDeletor(ICluster cluster, string keyspace, long timestamp)
        {
            this.timestamp = timestamp;
            this.cluster = cluster;
            this.keyspace = keyspace;
        }

        /// <summary>
        /// Delete a row with a specified key from a specified column family. The function succeeds even if
        /// the row does not exist.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family from which to delete the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level to be used
        /// </param>
        public void DeleteRow(string columnFamily, string rowKey, ConsistencyLevel consistencyLevel)
        {
            this.DeleteRow(columnFamily, rowKey.ToBytes(), consistencyLevel);
        }

        /// <summary>
        /// Delete a row with a specified key from a specified column family. The function succeeds even if
        /// the row does not exist.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family from which to delete the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level to be used
        /// </param>
        public void DeleteRow(string columnFamily, Guid rowKey, ConsistencyLevel consistencyLevel)
        {
            this.DeleteRow(columnFamily, rowKey.ToByteArray(), consistencyLevel);
        }

        /// <summary>
        /// Delete a row with a specified key from a specified column family. The function succeeds even if
        /// the row does not exist.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family from which to delete the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level to be used
        /// </param>
        public void DeleteRow(string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel)
        {
            ColumnPath path = new ColumnPath { Column_family = columnFamily };
            var mutate = new ExecutionBlock<string>(delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.remove(rowKey, path, this.timestamp, consistencyLevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
        }
    }
}
