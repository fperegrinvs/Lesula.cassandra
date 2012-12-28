namespace Lesula.Cassandra.FrontEnd
{
    using System.Collections.Generic;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.FrontEnd.Enumerators;

    /// <summary>
    /// Management operations need to be applied to a single node.
    /// See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
    /// </summary>
    public class ColumnFamilyManager
    {
        public const string CfdefTypeStandard = "Standard";
        public const string CfdefTypeSuper = "Super";

        public const string CfdefComparatorBytes = "BytesType";
        public const string CfdefComparatorAscii = "AsciiType";
        public const string CfdefComparatorUtf8 = "UTF8Type";
        public const string CfdefComparatorLong = "LongType";
        public const string CfdefComparatorLexicalUuid = "LexicalUUIDType";
        public const string CfdefComparatorTimeUuid = "TimeUUIDType";
        public const string CfdefComparatorInteger = "IntegerType";

        public const string CfdefValidationClassCounter = "CounterColumnType";

        /// <summary>
        /// The cluster.
        /// </summary>
        private readonly ICluster cluster;

        /// <summary>
        /// The keyspace.
        /// </summary>
        private readonly string keyspace;

        public ColumnFamilyManager(ICluster cluster, string keyspace)
        {
            this.cluster = cluster;
            this.keyspace = keyspace;
        }

        public void TruncateColumnFamily(string columnFamily)
        {
            var operation = new ExecutionBlock<string>(
        delegate(Cassandra.Iface myclient)
        {
            // Send batch mutation job to Thrift connection
            myclient.truncate(columnFamily);

            // Nothing to return
            return null;
        });
            this.cluster.Execute(operation, this.keyspace);
        }

        public static ColumnDef NewColumnDefinition(string name, bool indexed, ComparatorTypeEnum validationType = ComparatorTypeEnum.UTF8Type)
        {
            ColumnDef columnDef = new ColumnDef();
            columnDef.Validation_class = validationType.ToString();
            columnDef.Name = name.ToBytes();
            if (indexed)
            {
                columnDef.Index_type = IndexType.KEYS;
            }

            return columnDef;
        }

        /// <summary>
        /// Tries to add the column family, returns whether successful.
        /// Only throws on non-InvalidRequestException errors.
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="columnType">The column type</param>
        /// <param name="comparatorType">The comparator type</param>
        /// <param name="subComparatorType">The subcomparator type</param>
        /// <param name="columnDefs">The column definitions</param>
        /// <returns>Whether the operation was successful</returns>
        public bool TryAddColumnFamily(string name, ColumnTypeEnum columnType, ComparatorTypeEnum comparatorType, ComparatorTypeEnum subComparatorType = ComparatorTypeEnum.UTF8Type, List<ColumnDef> columnDefs = null)
        {
            try
            {
                AddColumnFamily(name, columnType, comparatorType, subComparatorType, columnDefs);
            }
            catch (InvalidRequestException)
            {
                return false;
            }

            return true;
        }

        public string AddColumnFamily(string name, ColumnTypeEnum columnType, ComparatorTypeEnum comparatorType, ComparatorTypeEnum subComparatorType = ComparatorTypeEnum.UTF8Type, List<ColumnDef> columnDefs = null)
        {
            CfDef cfDef = new CfDef
                {
                    Name = name,
                    Column_type = columnType.ToString().Replace("Counter", string.Empty),
                    Comparator_type = comparatorType.ToString(),
                    Keyspace = this.keyspace
                };

            if (columnType == ColumnTypeEnum.Super || columnType == ColumnTypeEnum.CounterSuper)
            {
                cfDef.Subcomparator_type = subComparatorType.ToString();
            }

            if (columnDefs != null)
            {
                foreach (var columnDef in columnDefs)
                {
                    if (columnDef.__isset.index_type)
                    {
                        columnDef.Index_name = name + "_" + columnDef.Name.ToUtf8String() + "idx";
                    }
                }

                cfDef.Column_metadata = columnDefs;
            }

            if (columnType == ColumnTypeEnum.CounterStandard || columnType == ColumnTypeEnum.CounterSuper)
            {
                cfDef.Default_validation_class = CfdefValidationClassCounter;
            }

            return AddColumnFamily(cfDef);
        }

        /// <summary>
        /// Tries to add the column family, returns whether successful. 
        /// Only throws on non-InvalidRequestException errors.
        /// </summary>
        /// <param name="columnFamilyDefinition">The column family definition</param>
        /// <returns>Whether the operation was successful</returns>
        public bool TryAddColumnFamily(CfDef columnFamilyDefinition)
        {
            try
            {
                AddColumnFamily(columnFamilyDefinition);
            }
            catch (InvalidRequestException)
            {
                return false;
            }

            return true;
        }

        public string AddColumnFamily(CfDef columnFamilyDefinition)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_add_column_family(columnFamilyDefinition));
            return this.cluster.Execute(operation, this.keyspace);
        }

        public string UpdateColumnFamily(CfDef columnFamilyDefinition)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_update_column_family(columnFamilyDefinition));
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Tries to drop the column family, returns whether successful.
        /// Only throws on non-InvalidRequestException errors.
        /// </summary>
        /// <param name="columnFamily">The column family</param>
        /// <returns>Whether the operation was successful</returns>
        public bool TryDropColumnFamily(string columnFamily)
        {
            try
            {
                this.DropColumnFamily(columnFamily);
            }
            catch (InvalidRequestException)
            {
                return false;
            }

            return true;
        }

        public string DropColumnFamily(string columnFamily)
        {
            var operation = new ExecutionBlock<string>(myclient => myclient.system_drop_column_family(columnFamily));
            return this.cluster.Execute(operation, this.keyspace);
        }
    }
}
