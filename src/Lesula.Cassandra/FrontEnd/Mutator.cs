// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Mutator.cs" company="">
//   
// </copyright>
// <summary>
//   Facilitates the mutation of data within a Cassandra keyspace: the desired mutations should first be specified by
//   calling methods such as
//   <code>
//   writeColumn(...)
//   </code>
//   , which should then be sent to Cassandra in a single batch by
//   calling
//   <code>
//   execute(...)
//   </code>
//   . After the desired batch of mutations has been executed, the
//   <code>
//   Mutator
//   </code>
//   object can not be re-used.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster;

    /// <summary>
    /// Facilitates the mutation of data within a Cassandra keyspace: the desired mutations should first be specified by
    ///   calling methods such as 
    /// <code>
    /// writeColumn(...)
    /// </code>
    /// , which should then be sent to Cassandra in a single batch by
    ///   calling 
    /// <code>
    /// execute(...)
    /// </code>
    /// . After the desired batch of mutations has been executed, the 
    /// <code>
    /// Mutator
    /// </code>
    /// object can not be re-used.
    /// </summary>
    public class Mutator
    {
        #region Constants and Fields

        /// <summary>
        ///   Used to indicate that the ttl property on column instances should not be set.
        /// </summary>
        public const int NoTtl = -1;

        /// <summary>
        /// The delete if null.
        /// </summary>
        private readonly bool deleteIfNull;

        /// <summary>
        /// The timestamp.
        /// </summary>
        private long Timestamp
        {
            get
            {
                return DateTimeOffset.UtcNow.ToTimestamp();
            }
        }

        /// <summary>
        /// The ttl.
        /// </summary>
        private readonly int ttl;

        /// <summary>
        /// The batch.
        /// </summary>
        private readonly Dictionary<byte[], Dictionary<string, List<Mutation>>> batch;

        /// <summary>
        /// The cluster.
        /// </summary>
        private readonly ICluster cluster;

        /// <summary>
        /// The keyspace.
        /// </summary>
        private readonly string keyspace;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Mutator"/> class. 
        /// Create a batch mutation operation.
        /// </summary>
        /// <param name="cluster">
        /// The cluster.
        /// </param>
        /// <param name="keyspace">
        /// The keyspace.
        /// </param>
        /// <param name="ttl">
        /// Time To Live das colunas criadas pelo método NewColumn.
        /// </param>
        public Mutator(ICluster cluster, string keyspace, int ttl = NoTtl)
            : this(false, ttl)
        {
            this.cluster = cluster;
            this.keyspace = keyspace;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Mutator"/> class. 
        /// Create a batch mutation operation.
        /// </summary>
        /// <param name="deleteIfNull">
        /// determine if null values on columns will result in a delete
        /// </param>
        /// <param name="ttl">
        /// the ttl (in seconds) that columns created using the various {@link #newColumn(Bytes, Bytes)}
        /// </param>
        /// <summary>
        /// helper methods will default to (null to indicate no default)
        /// </summary>
        public Mutator(bool deleteIfNull, int ttl = NoTtl)
        {
            this.deleteIfNull = deleteIfNull;
            this.ttl = ttl;
            this.batch = MutationsByKey.Create();
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Delete a column or super column
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the column or super column to delete.
        /// </param>
        /// <returns>
        /// The Mutator.
        /// </returns>
        public Mutator DeleteColumn(string colFamily, string rowKey, string colName)
        {
            this.DeleteColumn(colFamily, rowKey, colName.ToBytes());
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the column or super column to delete.
        /// </param>
        /// <returns>
        /// The mutator
        /// </returns>
        public Mutator DeleteColumn(string colFamily, string rowKey, byte[] colName)
        {
            this.DeleteColumn(colFamily, rowKey.ToBytes(), colName);
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the column or super column to delete.
        /// </param>
        public Mutator DeleteColumn(string colFamily, byte[] rowKey, byte[] colName)
        {
            this.DeleteColumns(colFamily, rowKey, new List<byte[]> { colName });
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, string rowKey, params byte[][] colNames)
        {
            this.DeleteColumns(colFamily, rowKey.ToBytes(), colNames.ToList());
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, byte[] rowKey, string[] colNames)
        {
            this.DeleteColumns(colFamily, rowKey, colNames.ToList());
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, byte[] rowKey, List<string> colNames)
        {
            this.DeleteColumns(colFamily, rowKey.ToUtf8String(), colNames.ToList());
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, string rowKey, List<string> colNames)
        {
            List<byte[]> colNameList = new List<byte[]>(colNames.Count);
            foreach (string colName in colNames)
            {
                colNameList.Add(colName.ToBytes());
            }

            this.DeleteColumns(colFamily, rowKey.ToBytes(), colNameList);
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, string rowKey, List<byte[]> colNames)
        {
            this.DeleteColumns(colFamily, rowKey.ToBytes(), colNames);
            return this;
        }

        /// <summary>
        /// Delete a list of columns or super columns.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colNames">
        /// The column and/or super column names to delete
        /// </param>
        public Mutator DeleteColumns(string colFamily, byte[] rowKey, List<byte[]> colNames)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumnNames(colNames);
            SlicePredicate pred = new SlicePredicate();
            pred.Column_names = colNames;
            var deletion = new Deletion();
            deletion.Timestamp = this.Timestamp;
            deletion.Predicate = pred;
            var mutation = new Mutation();
            mutation.Deletion = deletion;
            this.GetMutationList(colFamily, rowKey).Add(mutation);
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify.
        /// </param>
        /// <param name="subColName">
        /// The name of the sub-column to delete.
        /// </param>
        public Mutator DeleteSubColumn(string colFamily, string rowKey, string colName, string subColName)
        {
            this.DeleteSubColumn(
                colFamily,
                rowKey,
                colName.ToBytes(),
                subColName.ToBytes());
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify.
        /// </param>
        /// <param name="subColName">
        /// The name of the sub-column to delete.
        /// </param>
        public Mutator DeleteSubColumn(string colFamily, string rowKey, byte[] colName, string subColName)
        {
            this.DeleteSubColumn(colFamily, rowKey, colName, subColName.ToBytes());
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify.
        /// </param>
        /// <param name="subColName">
        /// The name of the sub-column to delete.
        /// </param>
        public Mutator DeleteSubColumn(string colFamily, string rowKey, string colName, byte[] subColName)
        {
            this.DeleteSubColumn(colFamily, rowKey, colName.ToBytes(), subColName);
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify.
        /// </param>
        /// <param name="subColName">
        /// The name of the sub-column to delete.
        /// </param>
        public Mutator DeleteSubColumn(string colFamily, string rowKey, byte[] colName, byte[] subColName)
        {
            this.DeleteSubColumn(colFamily, rowKey.ToBytes(), colName, subColName);
            return this;
        }

        /// <summary>
        /// Delete a column or super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify.
        /// </param>
        /// <param name="subColName">
        /// The name of the sub-column to delete.
        /// </param>
        public Mutator DeleteSubColumn(string colFamily, byte[] rowKey, byte[] colName, byte[] subColName)
        {
            List<byte[]> subColNames = new List<byte[]>(1);
            subColNames.Add(subColName);
            this.DeleteSubColumns(colFamily, rowKey, colName, subColNames);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete (empty value will result in all columns being removed)
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, string colName, List<string> subColNames)
        {
            this.DeleteSubColumns(colFamily, rowKey, colName.ToBytes(), subColNames);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete (empty value will result in all columns being removed)
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, byte[] colName, List<string> subColNames)
        {
            this.DeleteSubColumns(colFamily, rowKey.ToBytes(), colName, subColNames);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete (empty value will result in all columns being removed)
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, byte[] rowKey, byte[] colName, List<string> subColNames)
        {
            List<byte[]> subColNamesList = new List<byte[]>(subColNames.Count);
            foreach (string subColName in subColNames)
            {
                subColNamesList.Add(subColName.ToBytes());
            }

            this.DeleteSubColumns(colFamily, rowKey, colName, subColNamesList);
            return this;
        }

        /// <summary>
        /// Delete all sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, string colName)
        {
            this.DeleteSubColumns(
                colFamily,
                rowKey.ToBytes(),
                colName.ToBytes(),
                (List<byte[]>)null);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, string colName, List<byte[]> subColNames)
        {
            this.DeleteSubColumns(
                colFamily,
                rowKey.ToBytes(),
                colName.ToBytes(),
                subColNames);
            return this;
        }

        /// <summary>
        /// Delete all sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, byte[] colName)
        {
            this.DeleteSubColumns(
                colFamily, rowKey.ToBytes(), colName, (List<byte[]>)null);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, string rowKey, byte[] colName, List<byte[]> subColNames)
        {
            this.DeleteSubColumns(colFamily, rowKey.ToBytes(), colName, subColNames);
            return this;
        }

        /// <summary>
        /// Delete a list of sub-columns
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column to modify
        /// </param>
        /// <param name="subColNames">
        /// The sub-column names to delete
        /// </param>
        public Mutator DeleteSubColumns(string colFamily, byte[] rowKey, byte[] colName, List<byte[]> subColNames)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumnName(colName);
            if (subColNames != null)
            {
                Validation.validateColumnNames(subColNames);
            }

            var deletion = new Deletion();
            deletion.Timestamp = this.Timestamp;
            deletion.Super_column = colName;

            // CASSANDRA-1027 allows for a null predicate
            deletion.Predicate = (subColNames != null && subColNames.Count != 0)
                                     ? new SlicePredicate() { Column_names = subColNames }
                                     : null;
            Mutation mutation = new Mutation();
            mutation.Deletion = deletion;
            this.GetMutationList(colFamily, rowKey).Add(mutation);
            return this;
        }

        /// <summary>
        /// Execute the mutations that have been specified by sending them to Cassandra in a single batch.
        /// </summary>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level to be used
        /// </param>
        public void Execute(ConsistencyLevel consistencyLevel)
        {
            var mutate = new ExecutionBlock<string>(
                delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.batch_mutate(this.batch, consistencyLevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
            this.batch.Clear();
        }

        /// <summary>
        /// Execute the mutations that have been specified by sending them to Cassandra in a single batch.
        /// </summary>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level to be used
        /// </param>
        /// <returns>
        /// The execute debug.
        /// </returns>
        public Dictionary<byte[], Dictionary<string, List<Mutation>>> ExecuteDebug(ConsistencyLevel consistencyLevel)
        {
            var mutate = new ExecutionBlock<string>(
                delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.batch_mutate(this.batch, consistencyLevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
            return this.batch;
        }


        /// <summary>
        /// Get the default time stamp used by this 
        /// <code>
        /// Mutator
        /// </code>
        /// instance as a byte[].
        /// </summary>
        /// <param name="microsToMillis">
        /// If the time stamp is UTC microseconds (as is a self-constructed time stamp), whether to convert this into a standard milliseconds value
        /// </param>
        public byte[] GetMutationTimestamp(bool microsToMillis)
        {
            long result = this.Timestamp;
            if (microsToMillis)
            {
                result /= 1000;
            }

            return result.ToBytesBigEndian();
        }

        /// <summary>
        /// Get the raw time stamp value used by this 
        /// <code>
        /// Mutator
        /// </code>
        /// instance.
        /// </summary>
        /// <returns>
        /// The get mutation timestamp value.
        /// </returns>
        public long GetMutationTimestampValue()
        {
            return this.Timestamp;
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        public Column NewColumn(string colName, string colValue)
        {
            return this.NewColumn(
                colName.ToBytes(), colValue.ToBytes(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <param name="ttl">
        /// The time to live (in seconds) for the column
        /// </param>
        public Column NewColumn(string colName, string colValue, int ttl)
        {
            return this.NewColumn(
                colName.ToBytes(),
                colValue.ToBytes(),
                ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        public Column NewColumn(byte[] colName, string colValue)
        {
            return this.NewColumn(colName, colValue.ToBytes());
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <param name="ttl">
        /// The time to live (in seconds) for the column
        /// </param>
        public Column NewColumn(byte[] colName, string colValue, int ttl)
        {
            return this.NewColumn(colName, colValue.ToBytes(), ttl);
        }

        /// <summary>
        /// Create new Column object with an empty value and the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <returns>
        /// The new column.
        /// </returns>
        public Column NewColumn(string colName)
        {
            return this.NewColumn(colName.ToBytes(), new byte[0]);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(byte colName, DateTime colValue)
        {
            var value = colValue.Ticks.ToBytesBigEndian();
            return this.NewColumn(new[] { colName }, value);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, byte[] colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// The new column.
        /// </returns>
        public Column NewColumn(Guid colName, string colValue)
        {
            return this.NewColumn(colName.ToByteArray(), colValue.ToBytes(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <param name="ttl">
        /// The time to live (in seconds) for the column
        /// </param>
        public Column NewColumn(string colName, byte[] colValue, int ttl)
        {
            return this.NewColumn(colName.ToBytes(), colValue, ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, DateTime colValue)
        {
            var value = colValue.Ticks.ToBytesBigEndian();
            return this.NewColumn(colName.ToBytes(), value);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, bool colValue)
        {
            return this.NewColumn(colName.ToBytes(), BitConverter.GetBytes(colValue));
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, bool? colValue)
        {
            byte[] value = colValue.HasValue ? BitConverter.GetBytes(colValue.Value) : new byte[0];
            return this.NewColumn(colName.ToBytes(), value);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, double colValue)
        {
            return this.NewColumn(colName.ToBytes(), BitConverter.GetBytes(colValue));
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, Guid? colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.HasValue ? colValue.Value.ToByteArray() : new byte[0], this.ttl);
        }


        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, Guid colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.ToByteArray(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, long colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.ToBytesBigEndian(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, uint colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.ToBytesBigEndian(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, int colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.ToBytesBigEndian(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <returns>
        /// Coluna com os dados informados.
        /// </returns>
        public Column NewColumn(string colName, ulong colValue)
        {
            return this.NewColumn(colName.ToBytes(), colValue.ToBytesBigEndian(), this.ttl);
        }

        /// <summary>
        /// Create new Column object with an empty value and the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        public Column NewColumn(byte[] colName)
        {
            return this.NewColumn(colName, new byte[0], this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        public Column NewColumn(byte[] colName, byte[] colValue)
        {
            return this.NewColumn(colName, colValue, this.ttl);
        }

        /// <summary>
        /// Create new Column object with the time stamp passed to the constructor
        /// </summary>
        /// <param name="colName">
        /// The column name
        /// </param>
        /// <param name="colValue">
        /// The column value
        /// </param>
        /// <param name="ttl">
        /// The time to live (in seconds) for the column (-1 for default)
        /// </param>
        public Column NewColumn(byte[] colName, byte[] colValue, int ttl)
        {
            Column column = new Column();
            column.Name = colName;
            column.Value = colValue;
            column.Timestamp = this.Timestamp;

            if (ttl != NoTtl)
            {
                column.Ttl = ttl;
            }

            return column;
        }

        /// <summary>
        /// Create a new counter column.
        /// </summary>
        /// <param name="colName">
        /// The column name.
        /// </param>
        /// <param name="value">
        /// The value to increment/decrement the counter by.
        /// </param>
        /// <returns>
        /// The new counter column.
        /// </returns>
        public CounterColumn NewCounterColumn(byte[] colName, long value)
        {
            var column = new CounterColumn();
            column.Name = colName;
            column.Value = value;
            return column;
        }

        /// <summary>
        /// Create a new counter column.
        /// </summary>
        /// <param name="colName">
        /// The column name.
        /// </param>
        /// <param name="value">
        /// The value to increment/decrement the counter by.
        /// </param>
        /// <returns>
        /// The new counter column.
        /// </returns>
        public CounterColumn NewCounterColumn(string colName, long value)
        {
            var column = new CounterColumn();
            column.Name = colName.ToBytes();
            column.Value = value;
            return column;
        }

        /// <summary>
        /// Write a column value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumn(string colFamily, string rowKey, Column column)
        {
            this.WriteColumn(colFamily, rowKey.ToBytes(), column);
            return this;
        }

        /// <summary>
        /// Write a column value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumn(string colFamily, byte[] rowKey, Column column)
        {
            this.WriteColumn(colFamily, rowKey, column, this.deleteIfNull);
            return this;
        }

        /// <summary>
        /// Write a column value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumn(string colFamily, ulong rowKey, Column column)
        {
            this.WriteColumn(colFamily, rowKey.ToBytesBigEndian(), column, this.deleteIfNull);
            return this;
        }

        /// <summary>
        /// Write a column value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumn(string colFamily, Guid rowKey, Column column)
        {
            this.WriteColumn(colFamily, rowKey.ToByteArray(), column, this.deleteIfNull);
            return this;
        }

        /// <summary>
        /// Write a column value.  This method will automatically issue deletes if the deleteIfNullValue is true and the
        /// </summary>
        /// <summary>
        /// provided column does not have a value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <param name="deleteIfNullValue">
        /// If true and the provided column does NOT have value (as determined by the
        ///   {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
        ///   {@link #deleteColumn(string, Bytes, Bytes) delete} instead.
        /// </param>
        public Mutator WriteColumn(string colFamily, byte[] rowKey, Column column, bool deleteIfNullValue)
        {
            if (!deleteIfNullValue)
            {
                this.WriteColumnInternal(colFamily, rowKey, column);
            }
            else
            {
                if (column.__isset.value)
                {
                    this.WriteColumnInternal(colFamily, rowKey, column);
                }
                else
                {
                    this.DeleteColumn(colFamily, rowKey, column.Name);
                }
            }

            return this;
        }

        /// <summary>
        /// Insere uma coluna, sem usar o mutator.
        /// É mais performático quando trata-se de uma coluna individual
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <param name="clevel">
        /// The consistency level.
        /// </param>
        public void InsertColumn(string colFamily, ulong rowKey, Column column, ConsistencyLevel clevel)
        {
            var mutate = new ExecutionBlock<string>(
                delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.insert(rowKey.ToBytesBigEndian(), new ColumnParent { Column_family = colFamily }, column, clevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
        }

        /// <summary>
        /// Insere uma coluna, sem usar o mutator.
        /// É mais performático quando trata-se de uma coluna individual
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <param name="clevel">
        /// The consistency level.
        /// </param>
        public void InsertColumn(string colFamily, string rowKey, Column column, ConsistencyLevel clevel)
        {
            var mutate = new ExecutionBlock<string>(
                delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.insert(rowKey.ToBytes(), new ColumnParent { Column_family = colFamily }, column, clevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
        }

        /// <summary>
        /// Insere uma coluna, sem usar o mutator.
        /// É mais performático quando trata-se de uma coluna individual
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="column">
        /// The value of the column
        /// </param>
        /// <param name="clevel">
        /// The consistency level.
        /// </param>
        public void InsertColumn(string colFamily, byte[] rowKey, Column column, ConsistencyLevel clevel)
        {
            var mutate = new ExecutionBlock<string>(
                delegate(Cassandra.Iface myclient)
                {
                    // Send batch mutation job to Thrift connection
                    myclient.insert(rowKey,  new ColumnParent { Column_family = colFamily }, column, clevel);

                    // Nothing to return
                    return null;
                });

            this.cluster.Execute(mutate, this.keyspace);
        }

        /// <summary>
        /// Write a list of columns to a key
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="columns">
        /// The list of columns to write
        /// </param>
        public Mutator WriteColumns(string colFamily, string rowKey, List<Column> columns)
        {
            this.WriteColumns(colFamily, rowKey.ToBytes(), columns);
            return this;
        }

        /// <summary>
        /// Write a list of columns to a key
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="columns">
        /// The list of columns to write
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumns(string colFamily, Guid rowKey, List<Column> columns)
        {
            this.WriteColumns(colFamily, rowKey.ToByteArray(), columns);
            return this;
        }

        /// <summary>
        /// Write a list of columns to a key
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="columns">
        /// The list of columns to write
        /// </param>
        /// <returns>
        /// The Mutator
        /// </returns>
        public Mutator WriteColumns(string colFamily, ulong rowKey, List<Column> columns)
        {
            this.WriteColumns(colFamily, rowKey.ToBytesBigEndian(), columns);
            return this;
        }

        /// <summary>
        /// Write a list of columns to a key
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="columns">
        /// The list of columns to write
        /// </param>
        public Mutator WriteColumns(string colFamily, byte[] rowKey, List<Column> columns)
        {
            foreach (Column column in columns)
            {
                this.WriteColumn(colFamily, rowKey, column);
            }

            return this;
        }

        /// <summary>
        /// Write a list of columns to a key.  This method will automatically issue deletes if the deleteIfNullValue is true and the
        /// </summary>
        /// <summary>
        /// provided column does not have a value.
        /// </summary>
        /// <summary>
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="columns">
        /// The list of columns to write
        /// </param>
        /// <param name="deleteIfNullValue">
        /// If true and if the provided columns do NOT have value (as determined by the
        /// </param>
        /// <summary>
        /// {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
        /// </summary>
        /// <summary>
        /// {@link #deleteColumn(string, Bytes, Bytes) delete} instead.
        /// </summary>
        public Mutator WriteColumns(string colFamily, byte[] rowKey, List<Column> columns, bool deleteIfNullValue)
        {
            foreach (Column column in columns)
            {
                this.WriteColumn(colFamily, rowKey, column, deleteIfNullValue);
            }

            return this;
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Instância atual do mutator.
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, string key, ulong colName, long value)
        {
            return this.WriteCounterColumn(colFamily, key.ToBytes(), this.NewCounterColumn(colName.ToBytesBigEndian(), value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Instância atual do mutator.
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, int key, Guid colName, long value)
        {
            return this.WriteCounterColumn(colFamily, key.ToBytesBigEndian(), this.NewCounterColumn(colName.ToByteArray(), value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Instância atual do mutator.
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, int key, string colName, long value)
        {
            return this.WriteCounterColumn(colFamily, key.ToBytesBigEndian(), this.NewCounterColumn(colName.ToBytes(), value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Instância atual do mutator.
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, ulong key, string colName, long value)
        {
            return this.WriteCounterColumn(colFamily, key.ToBytesBigEndian(), this.NewCounterColumn(colName.ToBytes(), value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Instância atual do mutator.
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, string key, string colName, long value)
        {
            return this.WriteCounterColumn(colFamily, key.ToBytes(), this.NewCounterColumn(colName.ToBytes(), value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, byte[] rowKey, byte[] colName, long value)
        {
            return this.WriteCounterColumn(colFamily, rowKey, this.NewCounterColumn(colName, value));
        }

        /// <summary>
        /// The write counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="column">
        /// The column.
        /// </param>
        /// <returns>
        /// </returns>
        public Mutator WriteCounterColumn(string colFamily, byte[] rowKey, CounterColumn column)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumn(column);
            var cosc = new ColumnOrSuperColumn();
            cosc.Counter_column = column;
            var mutation = new Mutation();
            mutation.Column_or_supercolumn = cosc;
            this.GetMutationList(colFamily, rowKey).Add(mutation);

            return this;
        }

        /// <summary>
        /// The write counter columns.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="columns">
        /// The columns.
        /// </param>
        /// <returns>
        /// </returns>
        public Mutator WriteCounterColumns(string colFamily, byte[] rowKey, List<CounterColumn> columns)
        {
            foreach (CounterColumn column in columns)
            {
                this.WriteCounterColumn(colFamily, rowKey, column);
            }

            return this;
        }

        /// <summary>
        /// Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
        /// </summary>
        /// <summary>
        /// super column, then it is more efficient to use 
        /// <code>
        /// writeSubColumns
        /// </code>
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumn">
        /// The sub-column
        /// </param>
        public Mutator WriteSubColumn(string colFamily, Guid rowKey, string colName, Column subColumn)
        {
            this.WriteSubColumn(colFamily, rowKey.ToByteArray(), colName.ToBytes(), subColumn);
            return this;
        }

        /// <summary>
        /// Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
        /// </summary>
        /// <summary>
        /// super column, then it is more efficient to use 
        /// <code>
        /// writeSubColumns
        /// </code>
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumn">
        /// The sub-column
        /// </param>
        public Mutator WriteSubColumn(string colFamily, string rowKey, string colName, Column subColumn)
        {
            this.WriteSubColumn(colFamily, rowKey, colName.ToBytes(), subColumn);
            return this;
        }

        /// <summary>
        /// Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
        /// </summary>
        /// <summary>
        /// super column, then it is more efficient to use 
        /// <code>
        /// writeSubColumns
        /// </code>
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumn">
        /// The sub-column
        /// </param>
        public Mutator WriteSubColumn(string colFamily, string rowKey, byte[] colName, Column subColumn)
        {
            this.WriteSubColumn(colFamily, rowKey.ToBytes(), colName, subColumn);
            return this;
        }

        /// <summary>
        /// Write a single sub-column value to a super column. If you wish to write multiple sub-columns for a
        /// </summary>
        /// <summary>
        /// super column, then it is more efficient to use 
        /// <code>
        /// writeSubColumns
        /// </code>
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumn">
        /// The sub-column
        /// </param>
        public Mutator WriteSubColumn(string colFamily, byte[] rowKey, byte[] colName, Column subColumn)
        {
            this.WriteSubColumns(colFamily, rowKey, colName, new List<Column> { subColumn });
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        public Mutator WriteSubColumns(string colFamily, Guid rowKey, string colName, List<Column> subColumns)
        {
            this.WriteSubColumns(colFamily, rowKey.ToByteArray(), colName.ToBytes(), subColumns);
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        /// <returns>
        /// The mutator.
        /// </returns>
        public Mutator WriteSubColumns(string colFamily, string rowKey, string colName, List<Column> subColumns)
        {
            this.WriteSubColumns(colFamily, rowKey, colName.ToBytes(), subColumns);
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        /// <returns>
        /// The mutator.
        /// </returns>
        public Mutator WriteSubColumns(string colFamily, string rowKey, Guid colName, List<Column> subColumns)
        {
            this.WriteSubColumns(colFamily, rowKey.ToBytes(), colName.ToByteArray(), subColumns);
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        public Mutator WriteSubColumns(string colFamily, string rowKey, byte[] colName, List<Column> subColumns)
        {
            this.WriteSubColumns(colFamily, rowKey.ToBytes(), colName, subColumns);
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        public Mutator WriteSubColumns(string colFamily, byte[] rowKey, byte[] colName, List<Column> subColumns)
        {
            this.WriteSubColumns(colFamily, rowKey, colName, subColumns, this.deleteIfNull);
            return this;
        }

        /// <summary>
        /// Write multiple sub-column values to a super column.  This method will automatically delete sub columns if the
        ///   deleteIfNullValue is true and any of the sub columns do not have a value.
        /// </summary>
        /// <param name="colFamily">
        /// The column family
        /// </param>
        /// <param name="rowKey">
        /// The key of the row to modify
        /// </param>
        /// <param name="colName">
        /// The name of the super column
        /// </param>
        /// <param name="subColumns">
        /// A list of the sub-columns to write
        /// </param>
        /// <param name="deleteIfNullValue">
        /// If true and if the provided columns do NOT have values (as determined by the
        /// </param>
        /// <summary>
        /// {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
        /// </summary>
        /// <summary>
        /// call to {@link #deleteSubColumns(string, string, Bytes)} with the columns that
        /// </summary>
        /// <summary>
        /// have no values.
        /// </summary>
        public Mutator WriteSubColumns(
            string colFamily, byte[] rowKey, byte[] colName, List<Column> subColumns, bool deleteIfNullValue)
        {
            if (!deleteIfNullValue)
            {
                this.WriteSubColumnsInternal(colFamily, rowKey, colName, subColumns);
            }
            else
            {
                // figure out if we need to worry about columns with empty values
                bool isEmptyColumnPresent = false;
                foreach (Column subColumn in subColumns)
                {
                    if (!subColumn.__isset.value)
                    {
                        isEmptyColumnPresent = true;
                        break;
                    }
                }

                if (!isEmptyColumnPresent)
                {
                    this.WriteSubColumnsInternal(colFamily, rowKey, colName, subColumns);
                }
                else
                {
                    // separate out the columns that have a value from those that don't
                    List<Column> subColumnsWithValue = new List<Column>(subColumns.Count);
                    List<byte[]> subColumnsWithoutValue = new List<byte[]>(subColumns.Count);
                    foreach (Column subColumn in subColumns)
                    {
                        if (subColumn.__isset.value)
                        {
                            subColumnsWithValue.Add(subColumn);
                        }
                        else
                        {
                            subColumnsWithoutValue.Add(subColumn.Name);
                        }
                    }

                    this.WriteSubColumnsInternal(colFamily, rowKey, colName, subColumnsWithValue);
                    this.DeleteSubColumns(colFamily, rowKey, colName, subColumnsWithoutValue);
                }
            }

            return this;
        }

        /// <summary>
        /// The write sub counter column.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="subColumn">
        /// The sub column.
        /// </param>
        /// <returns>
        /// </returns>
        public Mutator WriteSubCounterColumn(string colFamily, byte[] rowKey, byte[] colName, CounterColumn subColumn)
        {
            this.WriteSubCounterColumns(colFamily, rowKey, colName, new List<CounterColumn> { subColumn });
            return this;
        }

        /// <summary>
        /// The write sub counter columns.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="subColumns">
        /// The sub columns.
        /// </param>
        /// <returns>
        /// </returns>
        public Mutator WriteSubCounterColumns(
            string colFamily, byte[] rowKey, byte[] colName, List<CounterColumn> subColumns)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumnName(colName);
            Validation.validateCounterColumns(subColumns);
            var scol = new CounterSuperColumn() { Name = colName, Columns = subColumns };
            var cosc = new ColumnOrSuperColumn();
            cosc.Counter_super_column = scol;
            var mutation = new Mutation();
            mutation.Column_or_supercolumn = cosc;
            this.GetMutationList(colFamily, rowKey).Add(mutation);
            return this;
        }

        /// <summary>
        /// Create a list of 
        /// <code>
        /// Column
        /// </code>
        /// objects.
        /// </summary>
        /// <param name="columns">
        /// The columns from which to compose the list
        /// </param>
        /// <returns>
        /// A list of 
        /// <code>
        /// Column
        /// </code>
        /// objects
        /// </returns>
        public List<Column> NewColumnList(params Column[] columns)
        {
            return columns.ToList();
        }

        #endregion

        #region Methods

        /// <summary>
        /// The get batch.
        /// </summary>
        /// <returns>
        /// </returns>
        protected Dictionary<byte[], Dictionary<string, List<Mutation>>> GetBatch()
        {
            return this.batch;
        }

        /// <summary>
        /// The get mutation list.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="key">
        /// The key.
        /// </param>
        /// <returns>
        /// </returns>
        protected MutationList GetMutationList(string colFamily, byte[] key)
        {
            Dictionary<string, List<Mutation>> mutsByCf;
            if (!this.batch.TryGetValue(key, out mutsByCf))
            {
                mutsByCf = new MutationsByCf();
                this.batch.Add(key, mutsByCf);
            }

            List<Mutation> mutList;
            if (!mutsByCf.TryGetValue(colFamily, out mutList))
            {
                mutList = new MutationList();
                mutsByCf.Add(colFamily, mutList);
            }

            return (MutationList)mutList;
        }

        /// <summary>
        /// The write column internal.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="column">
        /// The column.
        /// </param>
        private void WriteColumnInternal(string colFamily, byte[] rowKey, Column column)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumn(column);
            var cosc = new ColumnOrSuperColumn();
            cosc.Column = column;
            var mutation = new Mutation();
            mutation.Column_or_supercolumn = cosc;
            this.GetMutationList(colFamily, rowKey).Add(mutation);
        }

        /// <summary>
        /// The write sub columns internal.
        /// </summary>
        /// <param name="colFamily">
        /// The col family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <param name="subColumns">
        /// The sub columns.
        /// </param>
        private void WriteSubColumnsInternal(string colFamily, byte[] rowKey, byte[] colName, List<Column> subColumns)
        {
            Validation.safeGetRowKey(rowKey);
            Validation.validateColumnName(colName);
            Validation.validateColumns(subColumns);
            var scol = new SuperColumn();
            scol.Name = colName;
            scol.Columns = subColumns;
            var cosc = new ColumnOrSuperColumn();
            cosc.Super_column = scol;
            var mutation = new Mutation();
            mutation.Column_or_supercolumn = cosc;
            this.GetMutationList(colFamily, rowKey).Add(mutation);
        }

        #endregion

        /// <summary>
        /// The mutation list.
        /// </summary>
        protected class MutationList : List<Mutation>
        {
        }

        /// <summary>
        /// The mutations by cf.
        /// </summary>
        protected class MutationsByCf : Dictionary<string, List<Mutation>>
        {
        }

        /// <summary>
        /// The mutations by key.
        /// </summary>
        protected class MutationsByKey : Dictionary<byte[], Dictionary<string, List<Mutation>>>
        {
            /// <summary>
            /// Objeto usado para minimizar a quantidade de instância do ByteArrayComparer em memória.
            /// </summary>
            private static ByteArrayComparer byteArrayComparer = null;

            /// <summary>
            /// Singleton para o byteArrayComparer
            /// </summary>
            private static new ByteArrayComparer Comparer
            {
                get
                {
                    if (byteArrayComparer == null)
                    {
                        byteArrayComparer = new ByteArrayComparer();
                    }

                    return byteArrayComparer;
                }
            }

            /// <summary>
            /// Construtor padrão para o mapper de mutations
            /// </summary>
            /// <returns>
            /// instância do mapper
            /// </returns>
            public static Dictionary<byte[], Dictionary<string, List<Mutation>>> Create()
            {
                return new Dictionary<byte[], Dictionary<string, List<Mutation>>>(Comparer);
            }
        }
    }
}