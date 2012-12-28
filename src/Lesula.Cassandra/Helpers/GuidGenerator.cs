// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GuidGenerator.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   Gerador de Guid (a classe Guid do .NET só gera Guid da versão 4).
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Helpers
{
    using System;
    using System.Text;

    using Lesula.Cassandra.Helpers.Enumerators;

    /// <summary>
    /// Gerador de Guid (a classe Guid do .NET só gera Guid da versão 4).
    /// </summary>
    public static class GuidGenerator
    {
        /// <summary>
        /// Tamanho do nó em bits.
        /// </summary>
        public const byte NodeSize = 48;

        /// <summary>
        /// Valor máximo que um nó pode assumir.
        /// </summary>
        private const ulong MaxNode = 0xFFFFFFFFFFFF;

        /// <summary>
        /// Number of bytes in guid
        /// </summary>
        private const int ByteArraySize = 16;

        /// <summary>
        /// Byte usado para determinar a variante do layout do UUID.
        /// </summary>
        private const int VariantByte = 6;

        /// <summary>
        /// Mascara usada para determinar o byte que define a variante do UUID
        /// </summary>
        private const int VariantByteMask = 0x3f;

        /// <summary>
        /// Shift aplicado no cálculo da variante.
        /// </summary>
        private const int VariantByteShift = 0x80;

        /// <summary>
        /// Número do byte do Guid usado para identificar sua versão
        /// </summary>
        private const int VersionByte = 6;

        /// <summary>
        /// Mascara usada para identificar a versão do Guid.
        /// </summary>
        private const int VersionByteMask = 0x0f;

        /// <summary>
        /// Shift aplicado no calculo da versão do Guid.
        /// </summary>
        private const int VersionByteShift = 4;

        /// <summary>
        /// Byte onde se inicia o Timestamp.
        /// Segundo o RFC-4122: For UUID version 1, this is represented by Coordinated Universal Time (UTC)
        /// as a count of 100-nanosecond intervals since 00:00:00.00, 15 October 1582 (the date of Gregorian reform to the Christian calendar).
        /// </summary>
        private static readonly byte TimestampByte = 0;

        /// <summary>
        /// Sequência do clock. Segundo o RFC-4122
        /// For UUID version 1, the clock sequence is used to help avoid duplicates that could arise when the clock is set backwards in time
        /// or if the node ID changes.
        /// If the clock is set backwards, or might have been set backwards (e.g., while the system was powered off), and the UUID generator can
        ///  not be sure that no UUIDs were generated with timestamps larger than the value to which the clock was set, then the clock sequence has to be changed.
        /// If the previous value of the clock sequence is known, it can just be incremented; otherwise it should be set to a random or high-quality pseudo-random value.
        /// </summary>
        private static readonly byte GuidClockSequenceByte = 8;

        /// <summary>
        /// Identifica o gerador do Guid (neste caso vamos usar random). Segundo o RFC-4122
        /// For UUID version 1, the node field consists of an IEEE 802 MAC address, usually the host address.
        /// For systems with no IEEE address, a randomly or pseudo-randomly generated value may be used; see Section 4.5.  The multicast bit must 
        /// be set in such addresses, in order that they will never conflict with addresses obtained from network cards.
        /// </summary>
        private static readonly byte NodeByte = 10;

        /// <summary>
        /// Offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        /// </summary>
        private static readonly DateTime GregorianCalendarStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// random node that is 16 bytes
        /// </summary>
        private static readonly byte[] RandomNode;

        /// <summary>
        /// seed used for datetime
        /// </summary>
        private static DateTimePrecise dateTimeSeed;

        /// <summary>
        /// Gerador de números aleatórios.
        /// </summary>
        private static Random random = new Random();

        /// <summary>
        /// Initializes static members of the <see cref="GuidGenerator"/> class.
        /// </summary>
        static GuidGenerator()
        {
            RandomNode = new byte[6];
            random.NextBytes(RandomNode);
            dateTimeSeed = new DateTimePrecise(10);
        }

        /// <summary>
        /// Identifica a versão do Guid
        /// </summary>
        /// <param name="guid">
        /// Guid a ter a versão identificada.
        /// </param>
        /// <returns>
        /// Versão do guid.
        /// </returns>
        public static GuidVersion GetVersion(this Guid guid)
        {
            byte[] bytes = guid.ToByteArray();
            return (GuidVersion)((bytes[VersionByte] & 0xFF) >> VersionByteShift);
        }

        /// <summary>
        /// Retorna DateTime contido no Guid.
        /// </summary>
        /// <param name="guid">Guid a ter o DateTime recuperado</param>
        /// <returns>DateTime de criação do Guid.</returns>
        public static DateTime GetDateTime(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // reverse the version
            bytes[VersionByte] &= (byte)VersionByteMask;
            bytes[VersionByte] |= (byte)((byte)GuidVersion.TimeBased >> VersionByteShift);

            var timestampBytes = new byte[8];
            Array.Copy(bytes, TimestampByte, timestampBytes, 0, 8);
            Array.Reverse(timestampBytes);

            long timestamp = BitConverter.ToInt64(timestampBytes, 0);
            long ticks = timestamp + GregorianCalendarStart.Ticks;

            return new DateTime(ticks, DateTimeKind.Utc);
        }

        /// <summary>
        /// Gera um Guid do tipo 1 (TimeUUID)
        /// </summary>
        /// <returns>
        /// Guid do tipo 1 (TimeUUID)
        /// </returns>
        public static Guid GenerateTimeBasedGuid()
        {
            return GenerateTimeBasedGuid(dateTimeSeed.UtcNow, RandomNode);
        }

        /// <summary>
        /// Gera um Guid do tipo 1 (TimeUUID) utilizando uma data específica como timestamp.
        /// </summary>
        /// <param name="dateTime">
        /// DateTime usado como timestamp
        /// </param>
        /// <returns>
        /// Guid do tipo 1 (TimeUUID)
        /// </returns>
        public static Guid GenerateTimeBasedGuid(DateTime dateTime)
        {
            return GenerateTimeBasedGuid(dateTime, RandomNode);
        }

        /// <summary>
        /// Gera um guid com base em uma data específica como timestamp e identificador numérico
        /// </summary>
        /// <param name="node">
        /// Identificador numérico
        /// valor máximo é 0xFFFFFFFFFFFF
        /// </param>
        /// <returns>
        /// Guid do tipo 1 (TimeUUID)
        /// </returns>
        public static Guid GenerateTimeBasedGuid(ulong node)
        {
            return GenerateTimeBasedGuid(DateTime.UtcNow, node);
        }

        /// <summary>
        /// Gera um guid com base em uma data específica como timestamp e em um identificador textual
        /// </summary>
        /// <param name="node">Identificador textual com no máximo 6 caracteres e sem caracteres especiais.</param>
        /// <returns>Guid do tipo 1 (TimeUUID)</returns>
        public static Guid GenerateTimeBasedGuid(string node)
        {
            return GenerateTimeBasedGuid(DateTime.UtcNow, node.ToBytes());
        }

        /// <summary>
        /// Gera um guid com base em uma data específica como timestamp e identificador numérico
        /// </summary>
        /// <param name="dateTime">
        /// Timestamp do Guid
        /// </param> 
        /// <param name="node">
        /// Identificador numérico
        /// valor máximo é 0xFFFFFFFFFFFF
        /// </param>
        /// <returns>
        /// Guid do tipo 1 (TimeUUID)
        /// </returns>
        public static Guid GenerateTimeBasedGuid(DateTime dateTime, ulong node)
        {
            if (node > MaxNode)
            {
                throw new ArgumentException("node tem um valor máximo de 0xFFFFFFFFFFFF");
            }

            byte[] nodeArray = BitConverter.GetBytes(node);
            return GenerateTimeBasedGuid(dateTime, nodeArray);
        }

        /// <summary>
        /// Retorna node do Guid em formato ulong
        /// </summary>
        /// <param name="guid">
        /// Guid do tipo 1 (TimeUUID)
        /// </param>
        /// <returns>
        /// node que gerou o guid
        /// </returns>
        public static ulong GetNodeLong(Guid guid)
        {
            if (GetVersion(guid) != GuidVersion.TimeBased)
            {
                throw new ArgumentException("guid não é do tipo 1 (TimeUUID");
            }

            var bytes = guid.ToByteArray();
            return BitConverter.ToUInt64(bytes, NodeByte - 2) >> 16;
        }

        /// <summary>
        /// Retorna node do Guid em formato bytes
        /// </summary>
        /// <param name="guid">
        /// Guid do tipo 1 (TimeUUID)
        /// </param>
        /// <returns>
        /// node que gerou o guid
        /// </returns>
        public static byte[] GetNodeBytes(Guid guid)
        {
            if (GetVersion(guid) != GuidVersion.TimeBased)
            {
                throw new ArgumentException("guid não é do tipo 1 (TimeUUID");
            }

            var bytes = guid.ToByteArray();
            var node = new byte[6];
            Array.Copy(bytes, NodeByte, node, 0, 6);
            return node;
        }

        /// <summary>
        /// Retorna node do Guid em formato string
        /// </summary>
        /// <param name="guid">Guid do tipo 1 (TimeUUID)</param>
        /// <returns>node que gerou o guid</returns>
        public static string GetNodeString(Guid guid)
        {
            var bytes = GetNodeBytes(guid);
            return Encoding.UTF8.GetString(bytes).Replace("\0", string.Empty);
        }

        /// <summary>
        /// Gera um Guid do tipo 1 (TimeUUID) utilizando uma data específica como timestamp.
        /// </summary>
        /// <param name="dateTime">
        /// DateTime usado como timestamp
        /// </param>
        /// <param name="node">
        /// Identificador usado no Guid
        /// </param>
        /// <returns>
        /// Guid do tipo 1 (TimeUUID)
        /// </returns>
        public static Guid GenerateTimeBasedGuid(DateTime dateTime, byte[] node)
        {
            dateTime = dateTime.ToUniversalTime();
            long ticks = (dateTime - GregorianCalendarStart).Ticks;

            byte[] guid = new byte[ByteArraySize];
            byte[] clockSequenceBytes = BitConverter.GetBytes(Convert.ToInt16(Environment.TickCount % short.MaxValue));
            byte[] timestamp = BitConverter.GetBytes(ticks);
            Array.Reverse(timestamp);

            // copy node
            Array.Copy(node, 0, guid, NodeByte, Math.Min(6, node.Length));

            // copy clock sequence
            Array.Copy(clockSequenceBytes, 0, guid, GuidClockSequenceByte, Math.Min(2, clockSequenceBytes.Length));

            // copy timestamp
            Array.Copy(timestamp, 0, guid, TimestampByte, Math.Min(8, timestamp.Length));

            // set the variant
            guid[VariantByte] &= (byte)VariantByteMask;
            guid[VariantByte] |= (byte)VariantByteShift;

            // set the version
            guid[VersionByte] &= (byte)VersionByteMask;
            guid[VersionByte] |= (byte)((byte)GuidVersion.TimeBased << VersionByteShift);

            return new Guid(guid);
        }
    }
}