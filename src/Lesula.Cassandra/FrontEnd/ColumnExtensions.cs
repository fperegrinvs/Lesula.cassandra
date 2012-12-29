namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;

    using Apache.Cassandra;

    /// <summary>
    /// Extensões para conversão de bytes.
    /// </summary>
    public static class ColumnExtensions
    {
        /// <summary>
        /// Initializes static members of the <see cref="ColumnExtensions"/> class.
        /// </summary>
        static ColumnExtensions()
        {
            const int T = 1;
            byte[] buffer = BitConverter.GetBytes(T);
            isLittleEndian = buffer[0] == 1;
        }

        private static readonly bool isLittleEndian;

        public static void ReverseIfLittleEndian(this byte[] buffer)
        {
            if (isLittleEndian)
            {
                Array.Reverse(buffer);
            }
        }

        /// <summary>
        /// Converte array de bytes para DateTime
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// DateTime com a data descrita no array de bytes.
        /// </returns>
        public static DateTime? ToDateTimeNullable(this byte[] bytes)
        {
            if (bytes.Length > 0)
            {
                return new DateTime(bytes.ToInt64(), DateTimeKind.Utc);
            }

            return null;
        }

        /// <summary>
        /// Transforma lista de colunas em um dicionário chave/valor
        /// </summary>
        /// <param name="columns">lista de colunas</param>
        /// <returns>dicionário chave/valor</returns>
        public static Dictionary<string, byte[]> MapGeneric(this IList<Column> columns)
        {
            var dictionary = new Dictionary<string, byte[]>(columns.Count);

            foreach (var column in columns)
            {
                dictionary[column.Name.ToUtf8String()] = column.Value;
            }

            return dictionary;
        }

        /// <summary>
        /// Converte array de bytes para DateTime
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// DateTime com a data descrita no array de bytes.
        /// </returns>
        public static DateTime ToDateTime(this byte[] bytes)
        {
            return new DateTime(bytes.ToInt64(), DateTimeKind.Utc);
        }

        /// <summary>
        /// Converte array de bytes para Int64
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// Int64 representanfo os dados.
        /// </returns>
        public static long ToInt64(this byte[] bytes)
        {
            var x = bytes.Length;

            if (x == 8)
            {
                long v1 = bytes[0] << 0x18;
                v1 = v1 | bytes[1] << 0x10;
                v1 = v1 | bytes[2] << 0x08;
                v1 = v1 | bytes[3];

                long v = v1 << 0x20;

                v = v | (uint)(bytes[4] << 0x18 | bytes[5] << 0x10 | bytes[6] << 0x08 | bytes[7]);

                return v;
            }

            if (x == 7)
            {
                long v1 = bytes[0] << 0x10;
                v1 = v1 | bytes[1] << 0x08;
                v1 = v1 | bytes[2];

                long v = v1 << 0x20;

                v = v | (uint)(bytes[3] << 0x18 | bytes[4] << 0x10 | bytes[5] << 0x08 | bytes[6]);

                return v;
            }

            if (x == 6)
            {
                long v1 = bytes[0] << 0x08;
                v1 = v1 | bytes[1];

                long v = v1 << 0x20;

                v = v | (uint)(bytes[2] << 0x18 | bytes[3] << 0x10 | bytes[4] << 0x08 | bytes[5]);

                return v;
            }

            if (x == 5)
            {
                long v1 = bytes[0];

                long v = v1 << 0x20;

                v = v | (uint)(bytes[1] << 0x18 | bytes[2] << 0x10 | bytes[3] << 0x08 | bytes[4]);

                return v;
            }

            if (x == 4)
            {
                long v = bytes[0] << 0x18 | bytes[1] << 0x10 | bytes[2] << 0x08 | bytes[3];

                return v;
            }

            if (x == 3)
            {
                long v = bytes[0] << 0x10 | bytes[1] << 0x08 | bytes[2];

                return v;
            }

            if (x == 2)
            {
                long v = bytes[0] << 0x08 | bytes[1];

                return v;
            }

            return bytes[0];
        }

        /// <summary>
        /// Converte array de bytes para UInt64
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// UInt64 representanfo os dados.
        /// </returns>
        public static ulong ToUInt64(this byte[] bytes)
        {
            var x = bytes.Length;

            if (x == 8)
            {
                long v1 = (long)bytes[0] << 0x18;
                v1 = v1 | (bytes[1] << 0x10) | (bytes[2] << 0x08) | bytes[3];

                long v = v1 << 0x20;

                v = v | (long)bytes[4] << 0x18;
                v = v | (bytes[5] << 0x10) | (bytes[6] << 0x08) | bytes[7];

                return (ulong)v;
            }

            if (x == 7)
            {
                long v1 = bytes[0] << 0x10;
                v1 = v1 | (bytes[1] << 0x08) | bytes[2];

                long v = v1 << 0x20;

                v = v | (long)bytes[3] << 0x18;
                v = v | (bytes[4] << 0x10) | (bytes[5] << 0x08) | bytes[6];

                return (ulong)v;
            }

            if (x == 6)
            {
                long v1 = bytes[0] << 0x08;
                v1 = v1 | bytes[1];

                long v = v1 << 0x20;

                v = v | (long)bytes[2] << 0x18;
                v = v | (bytes[3] << 0x10) | (bytes[4] << 0x08) | bytes[5];

                return (ulong)v;
            }

            if (x == 5)
            {
                long v1 = bytes[0];

                long v = v1 << 0x20;

                v = v | (long)bytes[1] << 0x18;
                v = v | (bytes[2] << 0x10) | (bytes[3] << 0x08) | bytes[4];

                return (ulong)v;
            }

            if (x == 4)
            {
                long v = (long)bytes[0] << 0x18;
                v = v | (bytes[1] << 0x10) | (bytes[2] << 0x08) | bytes[3];

                return (ulong)v;
            }

            if (x == 3)
            {
                long v = bytes[0] << 0x10;
                v = v | (bytes[1] << 0x08) | bytes[2];

                return (ulong)v;
            }

            if (x == 2)
            {
                long v = bytes[0] << 0x08;
                v = v | bytes[1];

                return (ulong)v;
            }

            return bytes[0];
        }

        /// <summary>
        /// Converte array de bytes para um inteiro.
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// DateTime com a data descrita no array de bytes.
        /// </returns>
        public static int ToInt32(this byte[] bytes)
        {
            var y = bytes.Length;
            var x = y - 4;

            if (x < 0)
            {
                x = 0;
            }

            if (y >= 4)
            {
                return (bytes[x++] << 0x18) | (bytes[x++] << 0x10) | (bytes[x++] << 0x8) | bytes[x];
            }

            if (y == 3)
            {
                int v = bytes[x++] << 0x10;
                v = v | (bytes[x++] << 0x8);
                v = v | bytes[x];
                return v;
            }

            if (y == 2)
            {
                int v = bytes[x++] << 0x8;
                v = v | bytes[x];
                return v;
            }

            return bytes[x];
        }

        /// <summary>
        /// Converte array de bytes para um inteiro sem sinal.
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// DateTime com a data descrita no array de bytes.
        /// </returns>
        public static uint ToUInt32(this byte[] bytes)
        {
            var y = bytes.Length;
            var x = y - 4;

            if (x < 0)
            {
                x = 0;
            }

            if (y >= 4)
            {
                int v = bytes[x++] << 0x18 | (bytes[x++] << 0x10) | (bytes[x++] << 0x8) | bytes[x];
                return (uint)v;
            }

            if (y == 3)
            {
                int v = bytes[x++] << 0x10 | (bytes[x++] << 0x8) | bytes[x];
                return (uint)v;
            }

            if (y == 2)
            {
                int v = bytes[x++] << 0x8 | bytes[x];
                return (uint)v;
            }

            return bytes[x];
        }

        /// <summary>
        /// Converte array de bytes para Boolean
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// Boolean correspondente ao array de bytes.
        /// </returns>
        public static bool ToBool(this byte[] bytes)
        {
            return BitConverter.ToBoolean(bytes, 0);
        }

        /// <summary>
        /// Converte array de bytes para Guid
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// String codificada no array de bytes.
        /// </returns>
        public static Guid ToGuid(this byte[] bytes)
        {
            return new Guid(bytes);
        }

        /// <summary>
        /// Converte array de bytes para Guid?
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// String codificada no array de bytes.
        /// </returns>
        public static Guid? ToGuidNullable(this byte[] bytes)
        {
            if (bytes.Length == 0)
            {
                return null;
            }

            return new Guid(bytes);
        }

        /// <summary>
        /// Converte array de bytes para String
        /// </summary>
        /// <param name="bytes">
        /// The bytes.
        /// </param>
        /// <returns>
        /// String codificada no array de bytes.
        /// </returns>
        public static string ToUtf8String(this byte[] bytes)
        {
            return System.Text.Encoding.UTF8.GetString(bytes);
        }
    }
}
