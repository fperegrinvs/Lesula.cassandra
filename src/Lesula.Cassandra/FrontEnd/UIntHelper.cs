// --------------------------------------------------------------------------------------------------------------------
// <copyright file="UIntHelper.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Extension methods para enumeradores.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;

    /// <summary>
    /// Extension methods para enumeradores.
    /// </summary>
    public static class UIntHelper
    {
        /// <summary>
        /// Inverte os bits do inteiro.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <returns>
        /// Valor com a ordenação de bits invertida.
        /// </returns>
        public static uint ReverseBytes(this uint value)
        {
            return (value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 |
                   (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24;
        }

        /// <summary>
        /// Converte número para array de bytes.
        /// </summary>
        /// <param name="value">valor a ser convertido</param>
        /// <returns>array de bytes</returns>
        public static byte[] ToBytesBigEndian(this uint value)
        {
            value = ReverseBytes(value);
            return BitConverter.GetBytes(value);
        }
    }
}
