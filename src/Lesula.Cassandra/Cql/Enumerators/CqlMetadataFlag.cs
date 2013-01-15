namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    using System;

    [Flags]
    public enum CqlMetadataFlag
    {
        /// <summary>
        /// Global_tables_spec: if set, only one table spec (keyspace
        ///  and table name) is provided as global_table_spec. If not
        /// set, global_table_spec is not present.
        /// </summary>
        GlobalTable = 0x0001
    }
}
