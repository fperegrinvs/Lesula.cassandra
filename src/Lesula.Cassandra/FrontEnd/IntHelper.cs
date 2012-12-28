namespace Lesula.Cassandra
{
    using System;

    /// <summary>
    /// Helper para o int
    /// </summary>
    public static class IntHelper
    {
        /// <summary>
        /// Converte número para array de bytes.
        /// </summary>
        /// <param name="value">valor a ser convertido</param>
        /// <returns>array de bytes</returns>
        public static byte[] ToBytesBigEndian(this int value)
        {
            var bytes = BitConverter.GetBytes(value);
            Array.Reverse(bytes);
            return bytes;
        }

        /// <summary>
        /// Inverte os bits do inteiro.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Valor com a ordenação de bits invertida.
        /// </returns>
        public static int ReverseBytes(this int value)
        {
            if (value < 0)
            {
               value = (value & 0x000000FF) << 24 | (value & 0x0000FF00) << 8 |
                   (value & 0x00FF0000) >> 8 | (value & 0x7F000000) >> 24;
                value = value | 0x00000080;
            }

            return (value & 0x000000FF) << 24 | (value & 0x0000FF00) << 8 |
                   (value & 0x00FF0000) >> 8 | (value & 0x7F000000) >> 24;
        }
    }
}
