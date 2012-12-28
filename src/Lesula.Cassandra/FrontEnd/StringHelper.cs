namespace Lesula.Cassandra
{
    /// <summary>
    /// Helper para as strings
    /// </summary>
    public static class StringHelper
    {
        /// <summary>
        /// Converte uma string para um array de bytes
        /// </summary>
        /// <param name="s">
        /// String a ser convertida
        /// </param>
        /// <returns>
        /// Array de bytes do resultado
        /// </returns>
        public static byte[] ToBytes(this string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return new byte[0];
            }

            return System.Text.Encoding.UTF8.GetBytes(s);
        }
    }
}
