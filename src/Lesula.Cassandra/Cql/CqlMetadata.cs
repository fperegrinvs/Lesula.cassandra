// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CqlMetadata.cs" company="Lesula MapReduce Framework - http://github.com/lstern/Lesula.cassandra">
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
//   Defines the CqlMetadata type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Client.Cql
{
    using Lesula.Cassandra.Client.Cql.Enumerators;

    public class CqlMetadata
    {
        /// <summary>
        /// The bits of flags provides information on the
        /// formatting of the remaining informations. A flag is set if the bit
        /// corresponding to its `mask` is set.        
        /// </summary>
        public CqlMetadataFlag Flags { get; set; }

        /// <summary>
        /// representing the number of columns selected by the query this result is of.
        /// </summary>
        public int ColumnsCount { get; set; }

        /// <summary>
        /// Keyspace of the results
        /// </summary>
        public string Keyspace { get; set; }

        /// <summary>
        /// Column family of the results
        /// </summary>
        public string ColumnFamily { get; set; }

        /// <summary>
        /// Columns format
        /// </summary>
        public MetadataColumn[] Columns { get; set; }

        /// <summary>
        /// Number of rows (optional)
        /// </summary>
        public int? RowsCount { get; set; }
    }

    /// <summary>
    /// The metadata column.
    /// </summary>
    public class MetadataColumn
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataColumn"/> class.
        /// </summary>
        public MetadataColumn()
        {
            this.Type = new CqlType();
        }

        /// <summary>
        /// Keyspace of the results
        /// </summary>
        public string Keyspace { get; set; }

        /// <summary>
        /// Column family of the results
        /// </summary>
        public string ColumnFamily { get; set; }

        /// <summary>
        /// Name of the column
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Column Type
        /// </summary>
        public CqlType Type { get; set; }
    }

    /// <summary>
    /// The cql type.
    /// </summary>
    public class CqlType
    {
        /// <summary>
        /// The type of the column
        /// </summary>
        public CqlColumnType ColumnType { get; set; }

        /// <summary>
        /// full qualified classname of the custom type represented. 
        /// </summary>
        public string CustomType { get; set; }

        /// <summary>
        /// Type for lists, maps and sets
        /// </summary>
        public CqlType[] SubType { get; set; }
    }
}
