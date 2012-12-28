namespace Lesula.Cassandra
{
    using System;

    /// <summary>
    /// Extension methods para enumeradores.
    /// </summary>
    public static class ULongHelper
    {
        /// <summary>
        /// Conta quantos bits estão "ligados" em um inteiro
        /// </summary>
        /// <param name="number">
        /// The value.
        /// </param>
        /// <returns>
        /// Número de bits ativados na flag
        /// </returns>
        public static int CountBits(this ulong number)
        {
            number = number - ((number >> 1) & 0x5555555555555555UL);
            number = (number & 0x3333333333333333UL) + ((number >> 2) & 0x3333333333333333UL);
            return (int)(unchecked(((number + (number >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56);
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
        public static ulong ReverseBytes(this ulong value)
        {
            return (value & 0x00000000000000FFUL) << 56 | (value & 0x000000000000FF00UL) << 40 |
                   (value & 0x0000000000FF0000UL) << 24 | (value & 0x00000000FF000000UL) << 8 |
                   (value & 0x000000FF00000000UL) >> 8 | (value & 0x0000FF0000000000UL) >> 24 |
                   (value & 0x00FF000000000000UL) >> 40 | (value & 0xFF00000000000000UL) >> 56;
        }

        /// <summary>
        /// Converte número para array de bytes.
        /// </summary>
        /// <param name="value">valor a ser convertido</param>
        /// <returns>array de bytes</returns>
        public static byte[] ToBytesBigEndian(this ulong value)
        {
            value = ReverseBytes(value);
            return BitConverter.GetBytes(value);
        }
    }
}
