namespace Lesula.Cassandra.FrontEnd.Enumerators
{
    /// <summary>
    /// Tipos de coluna
    /// </summary>
    public enum ColumnTypeEnum
    {
        /// <summary>
        /// Coluna do tipo normal
        /// </summary>
        Standard,

        /// <summary>
        /// Coluna do tipo contador
        /// </summary>
        CounterStandard,

        /// <summary>
        /// Coluna do tipo contador
        /// </summary>
        CounterSuper,

        /// <summary>
        /// Coluna do tipo Super-Coluna
        /// </summary>
        Super
    }
}
