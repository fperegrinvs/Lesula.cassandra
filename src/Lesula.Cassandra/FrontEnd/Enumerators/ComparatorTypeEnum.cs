namespace Lesula.Cassandra.FrontEnd.Enumerators
{
    /// <summary>
    /// Enumerador com os tipos de comparadores de coluna do cassandra
    /// </summary>
    public enum ComparatorTypeEnum
    {
        /// <summary>
        /// Formato binário.
        /// </summary>
        BytesType,

        /// <summary>
        /// Texto em formato ASCII
        /// </summary>
        AsciiType,

        /// <summary>
        /// Texto no formato UTF8
        /// </summary>
        UTF8Type,

        /// <summary>
        /// Inteiro de 64 bits
        /// </summary>
        LongType,

        /// <summary>
        /// Guid versão 4
        /// </summary>
        LexicalUUIDType,

        /// <summary>
        /// Guid versão 1
        /// </summary>
        TimeUUIDType,

        /// <summary>
        /// Tipo inteiro 32 bits
        /// </summary>
        IntegerType,

        /// <summary>
        /// Coluna de contador
        /// </summary>
        CounterColumnType,
    }
}
