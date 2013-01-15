namespace Lesula.Cassandra.Client.Cql.Enumerators
{
    /// <summary>
    /// The first element of the body of a RESULT message is an [int] representing the
    /// `kind` of result. The rest of the body depends on the kind.
    /// </summary>
    public enum CqlResultKind
    {
        /// <summary>
        /// Void: for results carrying no information.
        /// </summary>
        Void = 0x0001,

        /// <summary>
        /// Rows: for results to select queries, returning a set of rows.
        /// </summary>
        Rows = 0x0002,

        /// <summary>
        /// Set_keyspace: the result to a `use` query.
        /// </summary>
        SetKeyspace = 0x0003,

        /// <summary>
        /// Prepared: result to a PREPARE message.
        /// </summary>
        Prepared = 0x0004,

        /// <summary>
        /// Schema_change: the result to a schema altering query.
        /// </summary>
        SchemaChange = 0x0005,
    }
}
