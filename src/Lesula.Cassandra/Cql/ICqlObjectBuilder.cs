namespace Lesula.Cassandra.Client.Cql
{
    using System.IO;

    public interface ICqlObjectBuilder<T>
    {
        /// <summary>
        /// Le o conteúdo de uma stream binária do cassandra
        /// </summary>
        /// <param name="s">stream binária</param>
        /// <returns>objeto codificado</returns>
        T ReadRows(Stream s, CqlMetadata metadata);
    }
}
