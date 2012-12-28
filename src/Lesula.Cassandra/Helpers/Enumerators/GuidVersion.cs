// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GuidVersion.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Guid Version
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Helpers.Enumerators
{
    /// <summary>
    /// Guid Version
    /// </summary>
    public enum GuidVersion
    {
        /// <summary>
        /// Conceptually, the original (version 1) generation scheme for UUIDs was to concatenate the UUID version with the MAC address of the computer 
        /// that is generating the UUID, and with the number of 100-nanosecond intervals since the adoption of the Gregorian calendar in the West
        /// </summary>
        TimeBased = 0x01,

        /// <summary>
        /// Version 2 UUIDs are similar to Version 1 UUIDs, with the upper byte of the clock sequence replaced by the identifier for a "local domain"
        /// (typically either the "POSIX UID domain" or the "POSIX GID domain") and the first 4 bytes of the timestamp replaced by the user's POSIX UID or 
        /// GID (with the "local domain" identifier indicating which it is)
        /// </summary>
        Reserved = 0x02,

        /// <summary>
        /// Version 3 UUIDs use a scheme deriving a UUID via MD5 from a URL, a fully qualified domain name, an object identifier, a distinguished name
        /// (DN as used in Lightweight Directory Access Protocol), or on names in unspecified namespaces. 
        /// </summary>
        NameBased = 0x03,

        /// <summary>
        /// Version 4 UUIDs use a scheme relying only on random numbers. This algorithm sets the version number as well as two reserved bits.
        /// All other bits are set using a random or pseudorandom data source.
        /// </summary>
        Random = 0x04
    }
}
