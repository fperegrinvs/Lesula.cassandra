namespace Lesula.Cassandra
{
    /// <summary>
    /// Helper para manipulação de booleanos
    /// </summary>
    public static class BooleanHelper
    {
        /// <summary>
        /// Retorna string  para o booleano usando '0' e '1' ao invés de false e true.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// '1' se o booleano é true, '0' em caso contrário
        /// </returns>
        public static string ToNumericString(this bool value)
        {
            return value ? "1" : "0";
        }
    }
}
