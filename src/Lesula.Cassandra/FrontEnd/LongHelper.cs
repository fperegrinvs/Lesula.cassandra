namespace Lesula.Cassandra
{
    using System;

    /// <summary>
    /// Helper para o tipo long
    /// </summary>
    public static class LongHelper
    {
        /// <summary>
        /// Converte número para array de bytes.
        /// </summary>
        /// <param name="value">valor a ser convertido</param>
        /// <returns>array de bytes</returns>
        public static byte[] ToBytesBigEndian(this long value)
        {
            return BitConverter.GetBytes(value.ReverseBytes());
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
        public static long ReverseBytes(this long value)
        {
            if (value < 0)
            {
                value = (value & 0x00000000000000FFL) << 56 | (value & 0x000000000000FF00L) << 40
                       | (value & 0x0000000000FF0000L) << 24 | (value & 0x00000000FF000000L) << 8
                       | (value & 0x000000FF00000000L) >> 8 | (value & 0x0000FF0000000000L) >> 24
                       | (value & 0x00FF000000000000L) >> 40 | (value & 0x7F00000000000000L) >> 56;
                return value | 0x0000000000000080L;
            }

            return (value & 0x00000000000000FFL) << 56 | (value & 0x000000000000FF00L) << 40
                   | (value & 0x0000000000FF0000L) << 24 | (value & 0x00000000FF000000L) << 8
                   | (value & 0x000000FF00000000L) >> 8 | (value & 0x0000FF0000000000L) >> 24
                   | (value & 0x00FF000000000000L) >> 40 | (value & 0x7F00000000000000L) >> 56;
        }
    }
}
