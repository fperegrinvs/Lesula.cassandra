// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Selector.cs" company="Many To One">
//   Copyright (c) ManyToOne Consultoria & Informatica Ltda. All rights reserved.
// </copyright>
// <summary>
//   Classe usada para facilitar a consulta de dados ao Cassandra.
//   Interface baseada no Pelops https://github.com/s7/scale7-pelops
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Cluster;

    /// <summary>
    /// Classe usada para facilitar a consulta de dados ao Cassandra.
    ///   Interface baseada no Pelops https://github.com/s7/scale7-pelops
    /// </summary>
    public class Selector : ISelector
    {
        // SlicePredicates constants for common internal uses
        #region Constants and Fields

        /// <summary>
        /// The column s_ predicat e_ all.
        /// </summary>
        private static readonly SlicePredicate ColumnPredicateAll = NewColumnsPredicateAll(false);

        /// <summary>
        /// The column s_ predicat e_ al l_ reversed.
        /// </summary>
        private static readonly SlicePredicate ColumnsPredicateAllReversed = NewColumnsPredicateAll(true);

        /// <summary>
        /// The cluster.
        /// </summary>
        private readonly ICluster cluster;

        /// <summary>
        /// The keyspace.
        /// </summary>
        private readonly string keyspace;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Selector"/> class.
        /// </summary>
        /// <param name="myCluster">
        /// The my cluster.
        /// </param>
        /// <param name="myKeyspace">
        /// The my keyspace.
        /// </param>
        public Selector(ICluster myCluster, string myKeyspace)
        {
            this.cluster = myCluster;
            this.keyspace = myKeyspace;
        }
        #endregion

        #region Public Methods

        /// <summary>
        /// Determines if a column with a particular name exist in the list of columns.
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <returns>
        /// Whether the column is present
        /// </returns>
        public static bool ColumnExists(List<Column> columns, byte[] colName)
        {
            return columns.Any(column => column.Name.SequenceEqual(colName));
        }

        /// <summary>
        /// Determines if a column with a particular name exist in the list of columns.
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <returns>
        /// Whether the column is present
        /// </returns>
        public static bool ColumnExists(List<Column> columns, string colName)
        {
            return ColumnExists(columns, colName.ToBytes());
        }

        /// <summary>
        /// Get the name of a column as a UTF8 string
        /// </summary>
        /// <param name="column">
        /// The column
        /// </param>
        /// <returns>
        /// The 
        /// <code>
        /// byte[]
        /// </code>
        /// name as a UTF8 string
        /// </returns>
        public static string GetColumnStringName(Column column)
        {
            return column.Name.ToUtf8String();
        }

        /// <summary>
        /// Get the value of a column as a UTF8 string
        /// </summary>
        /// <param name="column">
        /// The column containing the value
        /// </param>
        /// <returns>
        /// The 
        /// <code>
        /// byte[]
        /// </code>
        /// value as a UTF8 string
        /// </returns>
        public static string GetColumnStringValue(Column column)
        {
            return column.Value.ToUtf8String();
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <returns>
        /// The column value as a 
        /// <code>
        /// string
        /// </code>
        /// </returns>
        public static string GetColumnStringValue(List<Column> columns, string colName)
        {
            return GetColumnStringValue(columns, colName.ToBytes());
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <returns>
        /// The column value as a 
        /// <code>
        /// string
        /// </code>
        /// </returns>
        public static string GetColumnStringValue(List<Column> columns, byte[] colName)
        {
            foreach (Column column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Value.ToUtf8String();
                }
            }

            throw new ArgumentOutOfRangeException();
        }

        /// <summary>
        /// Get the time stamp of a column in a list of columns.
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the timestamp
        /// </param>
        /// <returns>
        /// The time stamp (the 
        /// <code>
        /// Mutator
        /// </code>
        /// object uses time stamps as microseconds)
        /// </returns>
        public static long GetColumnTimestamp(List<Column> columns, byte[] colName)
        {
            foreach (Column column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Timestamp;
                }
            }

            throw new ArgumentOutOfRangeException();
        }

        /// <summary>
        /// Get the time stamp of a column in a list of columns.
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the timestamp
        /// </param>
        /// <returns>
        /// The time stamp (the 
        /// <code>
        /// Mutator
        /// </code>
        /// object uses time stamps as microseconds)
        /// </returns>
        public static long GetColumnTimestamp(List<Column> columns, string colName)
        {
            return GetColumnTimestamp(columns, colName.ToBytes());
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static string GetColumnValue(List<Column> columns, string colName, string defaultValue)
        {
            return GetColumnValue(columns, colName.ToBytes(), defaultValue);
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static byte[] GetColumnValue(List<Column> columns, string colName, byte[] defaultValue)
        {
            return GetColumnValue(columns, colName.ToBytes(), defaultValue);
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static byte[] GetColumnValue(List<Column> columns, byte[] colName, byte[] defaultValue)
        {
            foreach (Column column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Value;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static string GetColumnValue(List<Column> columns, byte[] colName, string defaultValue)
        {
            foreach (Column column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Value.ToUtf8String();
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static byte[] GetColumnValue(List<Column> columns, byte[] colName)
        {
            foreach (Column column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Value;
                }
            }

            throw new ArgumentOutOfRangeException();
        }

        /// <summary>
        /// Get the value of a column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static byte[] GetColumnValue(List<Column> columns, string colName)
        {
            return GetColumnValue(columns, colName.ToBytes());
        }

        /// <summary>
        /// Get the value of a counter column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static long GetCountColumnValue(List<CounterColumn> columns, byte[] colName, long defaultValue)
        {
            foreach (CounterColumn column in columns)
            {
                if (column.Name.SequenceEqual(colName))
                {
                    return column.Value;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Get the value of a counter column in a list of columns
        /// </summary>
        /// <param name="columns">
        /// The list of columns
        /// </param>
        /// <param name="colName">
        /// The name of the column from which to retrieve the value
        /// </param>
        /// <param name="defaultValue">
        /// A default value to return if a column with the specified name is not present in the list
        /// </param>
        /// <returns>
        /// The column value
        /// </returns>
        public static long GetCountColumnValue(List<CounterColumn> columns, string colName, long defaultValue)
        {
            return GetCountColumnValue(columns, colName.ToBytes(), defaultValue);
        }

        /// <summary>
        /// Get a super column by name from a list of super columns
        /// </summary>
        /// <param name="superColumns">
        /// The list of super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <returns>
        /// The super column
        /// </returns>
        public static SuperColumn GetSuperColumn(List<SuperColumn> superColumns, byte[] superColName)
        {
            foreach (SuperColumn superColumn in superColumns)
            {
                if (superColumn.Name.SequenceEqual(superColName))
                {
                    return superColumn;
                }
            }

            throw new ArgumentOutOfRangeException();
        }

        /// <summary>
        /// Get a super column by name from a list of super columns
        /// </summary>
        /// <param name="superColumns">
        /// The list of super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <returns>
        /// The super column
        /// </returns>
        public static SuperColumn GetSuperColumn(List<SuperColumn> superColumns, string superColName)
        {
            return GetSuperColumn(superColumns, superColName.ToBytes());
        }

        /// <summary>
        /// The new column parent.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <returns>
        /// Returns new column parent.
        /// </returns>
        public static ColumnParent NewColumnParent(string columnFamily)
        {
            var parent = new ColumnParent { Column_family = columnFamily };
            return parent;
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="startName">
        /// The inclusive column start name of the range to select in the slice
        /// </param>
        /// <param name="finishName">
        /// The inclusive column end name of the range to select in the slice
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in reverse order
        /// </param>
        /// <param name="maxColCount">
        /// The maximum number of columns to return
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicate(
            byte[] startName, byte[] finishName, bool reversed, int maxColCount)
        {
            var predicate = new SlicePredicate
                {
                    Slice_range =
                        new SliceRange { Start = startName, Finish = finishName, Reversed = reversed, Count = maxColCount }
                };
            return predicate;
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="startName">
        /// The inclusive column start name of the range to select in the slice
        /// </param>
        /// <param name="finishName">
        /// The inclusive column end name of the range to select in the slice
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in reverse order
        /// </param>
        /// <param name="maxColCount">
        /// The maximum number of columns to return
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicate(
            string startName, string finishName, bool reversed, int maxColCount)
        {
            return NewColumnsPredicate(
                startName.ToBytes(),
                finishName.ToBytes(),
                reversed,
                maxColCount);
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="colNames">
        /// The specific columns names to select in the slice
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicate(params string[] colNames)
        {
            var asList = new List<byte[]>(32);
            asList.AddRange(colNames.Select(colName => colName.ToBytes()));

            var predicate = new SlicePredicate { Column_names = asList };
            return predicate;
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="colNames">
        /// The specific columns names to select in the slice
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicate(params byte[][] colNames)
        {
            List<byte[]> asList = colNames.ToList();
            var predicate = new SlicePredicate { Column_names = asList };
            return predicate;
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance that selects "all" columns with no imposed limit
        /// </summary>
        /// <param name="reversed">
        /// Whether the results should be returned in reverse order
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicateAll(bool reversed)
        {
            return NewColumnsPredicateAll(reversed, int.MaxValue);
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance that selects "all" columns
        /// </summary>
        /// <param name="reversed">
        /// Whether the results should be returned in reverse order
        /// </param>
        /// <param name="maxColCount">
        /// The maximum number of columns to return
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        public static SlicePredicate NewColumnsPredicateAll(bool reversed, int maxColCount)
        {
            var predicate = new SlicePredicate
                {
                    Slice_range =
                        new SliceRange { Start = new byte[0], Finish = new byte[0], Reversed = reversed, Count = maxColCount }
                };
            return predicate;
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// IndexClause
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="expressions">
        /// Index value lookup expressions
        /// </param>
        /// <returns>
        /// The new IndexClause
        /// </returns>
        public static IndexClause NewIndexClause(params IndexExpression[] expressions)
        {
            return NewIndexClause(new byte[0], 10000, expressions);
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// IndexClause
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="startName">
        /// The inclusive column start name of the index range to select in the slice
        /// </param>
        /// <param name="count">
        /// The maximum number of rows to return
        /// </param>
        /// <param name="expressions">
        /// Index value lookup expressions
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// IndexClause
        /// </code>
        /// </returns>
        public static IndexClause NewIndexClause(string startName, int count, params IndexExpression[] expressions)
        {
            return NewIndexClause(startName.ToBytes(), count, expressions);
        }

        /// <summary>
        /// Create a new IndexClause instance.
        /// </summary>
        /// <param name="startName">
        /// The inclusive column start name of the index range to select in the slice
        /// </param>
        /// <param name="count">
        /// The maximum number of rows to return
        /// </param>
        /// <param name="expressions">
        /// Index value lookup expressions
        /// </param>
        /// <returns>
        /// The new IndexClause
        /// </returns>
        public static IndexClause NewIndexClause(byte[] startName, int count, params IndexExpression[] expressions)
        {
            return new IndexClause { Expressions = expressions.ToList(), Start_key = startName, Count = count };
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// </returns>
        public static IndexExpression NewIndexExpression(byte[] colName, IndexOperator op, byte[] value)
        {
            return new IndexExpression { Column_name = colName, Op = op, Value = value };
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// </returns>
        public static IndexExpression NewIndexExpression(string colName, IndexOperator op, byte[] value)
        {
            return NewIndexExpression(colName.ToBytes(), op, value);
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// IndexExpression
        /// </code>
        /// </returns>
        public static IndexExpression NewIndexExpression(string colName, IndexOperator op, string value)
        {
            return NewIndexExpression(colName.ToBytes(), op, value.ToBytes());
        }

        /// <summary>
        /// Create a new IndexExpression instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new IndexExpression
        /// </returns>
        public static IndexExpression NewIndexExpression(string colName, IndexOperator op, ulong value)
        {
            return NewIndexExpression(colName.ToBytes(), op, value.ToBytesBigEndian());
        }

        /// <summary>
        /// Create a new IndexExpression instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new IndexExpression
        /// </returns>
        public static IndexExpression NewIndexExpression(string colName, IndexOperator op, Guid value)
        {
            return NewIndexExpression(colName.ToBytes(), op, value.ToByteArray());
        }

        /// <summary>
        /// Create a new IndexExpression instance.
        /// </summary>
        /// <param name="colName">
        /// The name of the column
        /// </param>
        /// <param name="op">
        /// The index expression operator (for now only EQ works)
        /// </param>
        /// <param name="value">
        /// Lookup value
        /// </param>
        /// <returns>
        /// The new IndexExpression
        /// </returns>
        public static IndexExpression NewIndexExpression(string colName, IndexOperator op, int value)
        {
            return NewIndexExpression(colName.ToBytes(), op, value.ToBytesBigEndian());
        }

        /// <summary>
        /// Create a new KeyRange instance.
        /// </summary>
        /// <param name="startKey">
        /// The inclusive start key of the range
        /// </param>
        /// <param name="finishKey">
        /// The inclusive finish key of the range
        /// </param>
        /// <param name="maxKeyCount">
        /// The maximum number of keys to be scanned
        /// </param>
        /// <returns>
        /// The new KeyRange instance
        /// </returns>
        public static KeyRange NewKeyRange(string startKey, string finishKey, int maxKeyCount)
        {
            return NewKeyRange(
                startKey.ToBytes(),
                finishKey.ToBytes(),
                maxKeyCount);
        }

        /// <summary>
        /// Create a new KeyRange instance.
        /// </summary>
        /// <param name="startKey">
        /// The inclusive start key of the range
        /// </param>
        /// <param name="finishKey">
        /// The inclusive finish key of the range
        /// </param>
        /// <param name="maxKeyCount">
        /// The maximum number of keys to be scanned
        /// </param>
        /// <returns>
        /// The new KeyRange instance
        /// </returns>
        public static KeyRange NewKeyRange(ulong startKey, ulong finishKey, int maxKeyCount)
        {
            return NewKeyRange(startKey.ToBytesBigEndian(), finishKey.ToBytesBigEndian(), maxKeyCount);
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// KeyRange
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="startKey">
        /// The inclusive start key of the range
        /// </param>
        /// <param name="finishKey">
        /// The inclusive finish key of the range
        /// </param>
        /// <param name="maxKeyCount">
        /// The maximum number of keys to be scanned
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// KeyRange
        /// </code>
        /// instance
        /// </returns>
        public static KeyRange NewKeyRange(byte[] startKey, byte[] finishKey, int maxKeyCount)
        {
            var keyRange = new KeyRange { Count = maxKeyCount, Start_key = startKey, End_key = finishKey };
            return keyRange;
        }

        /// <summary>
        /// Gets the key range all.
        /// </summary>
        public static KeyRange KeyRangeAll
        {
            get
            {
                return new KeyRange { Count = 10000, Start_key = new byte[0], End_key = new byte[0] };
            }
        }

        /// <summary>
        /// Create a new 
        /// <code>
        /// KeyRange
        /// </code>
        /// instance.
        /// </summary>
        /// <param name="startFollowingKey">
        /// The exclusive start key of the ring range
        /// </param>
        /// <param name="finishKey">
        /// The inclusive finish key of the range (can be less than 
        /// <code>
        /// startFollowing
        /// </code>
        /// )
        /// </param>
        /// <param name="maxKeyCount">
        /// The maximum number of keys to be scanned
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// KeyRange
        /// </code>
        /// instance
        /// </returns>
        public static KeyRange NewKeyRingRange(string startFollowingKey, string finishKey, int maxKeyCount)
        {
            var keyRange = new KeyRange { Count = maxKeyCount, Start_token = startFollowingKey, End_token = finishKey };
            return keyRange;
        }

        /// <summary>
        /// Determines if a super column with a particular name exist in the list of super columns.
        /// </summary>
        /// <param name="superColumns">
        /// The list of super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <returns>
        /// Whether the super column is present
        /// </returns>
        public static bool SuperColumnExists(List<SuperColumn> superColumns, string superColName)
        {
            return SuperColumnExists(superColumns, superColName.ToBytes());
        }

        /// <summary>
        /// Determines if a super column with a particular name exist in the list of super columns.
        /// </summary>
        /// <param name="superColumns">
        /// The list of super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <returns>
        /// Whether the super column is present
        /// </returns>
        public static bool SuperColumnExists(List<SuperColumn> superColumns, byte[] superColName)
        {
            return superColumns.Any(superColumn => superColumn.Name.SequenceEqual(superColName));
        }

        /// <summary>
        /// Get the count of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the columns
        /// </returns>
        public int GetColumnCount(string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily), rowKey, ColumnPredicateAll, consistencyLevel);
        }

        /// <summary>
        /// Get the count of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the columns
        /// </returns>
        public int GetColumnCount(string columnFamily, byte[] rowKey, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily), rowKey, predicate, consistencyLevel);
        }

        /// <summary>
        /// Get the count of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the columns
        /// </returns>
        public int GetColumnCount(string columnFamily, string rowKey, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily),
                rowKey.ToBytes(),
                ColumnPredicateAll,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the columns
        /// </returns>
        public int GetColumnCount(string columnFamily, string rowKey, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily), rowKey.ToBytes(), predicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested Column
        /// </returns>
        public Column GetColumnFromRow(string columnFamily, ulong rowKey, string colName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnFromRow(
                columnFamily, rowKey.ToBytesBigEndian(), colName.ToBytes(), consistencyLevel);
        }

        /// <summary>
        /// Retrieve a column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested Column
        /// </returns>
        public Column GetColumnFromRow(string columnFamily, Guid rowKey, string colName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnFromRow(
                columnFamily, rowKey.ToByteArray(), colName.ToBytes(), consistencyLevel);
        }

        /// <summary>
        /// Retrieve a column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetColumnFromRow(string columnFamily, string rowKey, string colName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnFromRow(
                columnFamily, rowKey, colName.ToBytes(), consistencyLevel);
        }

        /// <summary>
        /// Retrieve a column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetColumnFromRow(string columnFamily, string rowKey, byte[] colName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnFromRow(
                columnFamily, rowKey.ToBytes(), colName, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetColumnFromRow(string columnFamily, byte[] rowKey, byte[] colName, ConsistencyLevel consistencyLevel)
        {
            ColumnPath cp = NewColumnPath(columnFamily, null, colName);

            var operation = new ExecutionBlock<Column>(
                delegate(Cassandra.Iface myclient)
                {
                    ColumnOrSuperColumn cosc = myclient.get(Validation.safeGetRowKey(rowKey), cp, consistencyLevel);
                    return cosc.Column;
                });

            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, string rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, string rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, ulong rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey.ToBytesBigEndian(), colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, ulong rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey.ToBytesBigEndian(), ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, byte[] rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetColumnsFromRow(
            string columnFamily, byte[] rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the columns
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a range of rows.
        ///   The method returns a map from the keys of rows in the specified range to lists of columns from the rows. The map
        ///   returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        ///   Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            string columnFamily, KeyRange keyRange, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a range of rows.
        ///   The method returns a map from the keys of rows in the specified range to lists of columns from the rows. The map
        ///   returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        ///   Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetCounterColumnsFromRows(
            string columnFamily, KeyRange keyRange, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a range of rows.
        ///   The method returns a map from the keys of rows in the specified range to lists of columns from the rows. The map 
        ///   returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by 
        ///   Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            string columnFamily, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="count">
        /// Quantidade de registros retornados.
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetColumnsAllRowsUtf8Keys(
            string columnFamily, int count, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                columnFamily,
                new KeyRange { Count = count, Start_key = new byte[0], End_key = new byte[0] },
                false,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the columns
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(NewColumnParent(columnFamily), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a range of rows.
        ///   The method returns a map from the keys of rows in the specified range to lists of columns from the rows. The map
        ///   returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        ///   Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(NewColumnParent(columnFamily), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a counter column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colName">
        /// The name of the column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public CounterColumn GetCounterColumnFromRow(
            string columnFamily, byte[] rowKey, byte[] colName, ConsistencyLevel consistencyLevel)
        {
            ColumnPath cp = NewColumnPath(columnFamily, null, colName);

            var operation = new ExecutionBlock<CounterColumn>(
                delegate(Cassandra.Iface myclient)
                {
                    ColumnOrSuperColumn cosc = myclient.get(Validation.safeGetRowKey(rowKey), cp, consistencyLevel);
                    return cosc.Counter_column;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get counter column value from row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colName">
        /// The column name.
        /// </param>
        /// <param name="consistencyLevel">
        /// The consistency level.
        /// </param>
        /// <returns>
        /// Returns the get counter column value from row.
        /// </returns>
        public long GetCounterColumnValueFromRow(
            string columnFamily, byte[] rowKey, byte[] colName, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnFromRow(columnFamily, rowKey, colName, consistencyLevel).Value;
        }

        /// <summary>
        /// Retrieve all counter columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<CounterColumn> GetCounterColumnsFromRow(
            string columnFamily, byte[] rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRow(
                NewColumnParent(columnFamily), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve counter columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<CounterColumn> GetCounterColumnsFromRow(
            string columnFamily, byte[] rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRow(NewColumnParent(columnFamily), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// The get counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="reversed">
        /// The reversed.
        /// </param>
        /// <param name="consistencyLevel">
        /// The consistency level level.
        /// </param>
        /// <returns>
        /// counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetCounterColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// The get counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetCounterColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(NewColumnParent(columnFamily), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// The get counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// counter columns from rows.
        /// </returns>
        public List<KeyValuePair<string, List<CounterColumn>>> GetCounterColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var keys = new List<byte[]>();
            Validation.validateRowKeysUtf8(rowKeys).ForEach(o => keys.Add(o.ToBytes()));

            var operation = new ExecutionBlock<List<KeyValuePair<string, List<CounterColumn>>>>(
            delegate(Cassandra.Iface myclient)
            {
                var apiResults = myclient.multiget_slice(keys, NewColumnParent(columnFamily), colPredicate, consistencyLevel);

                return apiResults.Select(apiResult =>
                    new KeyValuePair<string, List<CounterColumn>>(apiResult.Key.ToUtf8String(), ToCounterColumnList(apiResult.Value))).ToList();
            });

            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all columns from a range of indexed rows using its secondary index.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of indexed rows in the specified range to lists of columns from the rows. The map
        /// returned is a LinkedHashMap  and its key iterator proceeds in the order that the key data was returned by
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column parent containing the rows
        /// </param>
        /// <param name="indexClause">
        /// A index clause
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetIndexedColumns(
            string columnFamily, IndexClause indexClause, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetIndexedColumns(
                NewColumnParent(columnFamily), indexClause, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a range of indexed rows using its secondary index.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of indexed rows in the specified range to lists of columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="colParent">
        /// The column parent containing the rows
        /// </param>
        /// <param name="indexClause">
        /// A index clause
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetIndexedColumns(
            string colParent, IndexClause indexClause, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetIndexedColumns(NewColumnParent(colParent), indexClause, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all columns from a range of indexed rows using its secondary index.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of indexed rows in the specified range to lists of columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="colParent">
        /// The column parent
        /// </param>
        /// <param name="indexClause">
        /// A index key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetIndexedColumns(
            ColumnParent colParent, IndexClause indexClause, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetIndexedColumns(colParent, indexClause, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve columns from a range of indexed rows using its secondary index.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of indexed rows in the specified range to lists of columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a LinkedHashMap and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="colParent">
        /// The column parent
        /// </param>
        /// <param name="indexClause">
        /// A index key range selecting the rows
        /// </param>
        /// <param name="colPredicate">
        /// The column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetIndexedColumns(
            ColumnParent colParent, IndexClause indexClause, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<KeyValuePair<byte[], List<Column>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<KeySlice> apiResult = myclient.get_indexed_slices(
                        colParent, indexClause, colPredicate, consistencyLevel);
                    var result = new List<KeyValuePair<byte[], List<Column>>>(apiResult.Count);

                    result.AddRange(
                        from ks in apiResult
                        let coscList = ks.Columns
                        let colList = ToColumnList(coscList)
                        select new KeyValuePair<byte[], List<Column>>(ks.Key, colList));

                    return result;
                });

            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve a page of column names composed from a segment of the sequence of columns in a row.
        /// </summary>
        /// <summary>
        /// This method is handy for performing <a href="https://github.com/ericflo/twissandra/">Twissandra</a> style
        /// </summary>
        /// <summary>
        /// one to many lookups.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest column name greater than this value. Pass null to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of column names
        /// </returns>
        public List<byte[]> GetPageOfColumnNamesFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            List<Column> columns = this.GetPageOfColumnsFromRow(
                columnFamily, rowKey, startBeyondName, reversed, count, consistencyLevel);

            // transform to a list of column names
            var columnNames = new List<byte[]>(columns.Count);
            columnNames.AddRange(columns.Select(column => column.Name));

            return columnNames;
        }

        /// <summary>
        /// Retrieve a page of columns composed from a segment of the sequence of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of columns
        /// </returns>
        public List<Column> GetPageOfColumnsFromRow(
            string columnFamily,
            string rowKey,
            byte[] startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName,
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of columns composed from a segment of the sequence of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of columns
        /// </returns>
        public List<Column> GetPageOfColumnsFromRow(
            string columnFamily,
            string rowKey,
            string startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName.ToBytes(),
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of columns composed from a segment of the sequence of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of columns
        /// </returns>
        public List<Column> GetPageOfColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            string startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfColumnsFromRow(
                columnFamily,
                rowKey,
                startBeyondName.ToBytes(),
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of columns composed from a segment of the sequence of columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of columns
        /// </returns>
        public List<Column> GetPageOfColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            SlicePredicate predicate;
            if (startBeyondName == null)
            {
                predicate = NewColumnsPredicateAll(reversed, count);
                return this.GetColumnsFromRow(columnFamily, rowKey, predicate, consistencyLevel);
            }

            int incrementedCount = count + 1;

            // cassandra will return the start row but the user is expecting a page of results beyond that point
            predicate = NewColumnsPredicate(startBeyondName, new byte[0], reversed, incrementedCount);
            List<Column> columns = this.GetColumnsFromRow(columnFamily, rowKey, predicate, consistencyLevel);
            if (columns.Count > 0)
            {
                Column first = columns.First();
                if (first.Name.SequenceEqual(startBeyondName))
                {
                    return columns.GetRange(1, columns.Count);
                }

                if (columns.Count == incrementedCount)
                {
                    return columns.GetRange(0, columns.Count - 1);
                }
            }

            return columns;
        }

        /// <summary>
        /// Retrieve a page of super columns composed from a segment of the sequence of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of super columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public List<SuperColumn> GetPageOfSuperColumnsFromRow(
            string columnFamily,
            string rowKey,
            byte[] startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfSuperColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName,
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of super columns composed from a segment of the sequence of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of super columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public List<SuperColumn> GetPageOfSuperColumnsFromRow(
            string columnFamily,
            string rowKey,
            string startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfSuperColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName.ToBytes(),
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of super columns composed from a segment of the sequence of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of super columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public List<SuperColumn> GetPageOfSuperColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            string startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetPageOfSuperColumnsFromRow(
                columnFamily,
                rowKey,
                startBeyondName.ToBytes(),
                reversed,
                count,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a page of super columns composed from a segment of the sequence of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="count">
        /// The maximum number of super columns that can be retrieved by the scan
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public List<SuperColumn> GetPageOfSuperColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] startBeyondName,
            bool reversed,
            int count,
            ConsistencyLevel consistencyLevel)
        {
            if (startBeyondName == null)
            {
                SlicePredicate predicate = NewColumnsPredicateAll(reversed, count);
                return this.GetSuperColumnsFromRow(columnFamily, rowKey, predicate, consistencyLevel);
            }
            else
            {
                int incrementedCount = count + 1;

                // cassandra will return the start row but the user is expecting a page of results beyond that point
                SlicePredicate predicate = NewColumnsPredicate(
                    startBeyondName, new byte[0], reversed, incrementedCount);
                List<SuperColumn> columns = this.GetSuperColumnsFromRow(columnFamily, rowKey, predicate, consistencyLevel);
                if (columns.Count > 0)
                {
                    SuperColumn first = columns.First();
                    if (first.Name.SequenceEqual(startBeyondName))
                    {
                        return columns.GetRange(1, columns.Count);
                    }

                    if (columns.Count == incrementedCount)
                    {
                        return columns.GetRange(0, columns.Count - 1);
                    }
                }

                return columns;
            }
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(string columnFamily, string rowKey, byte[] superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName),
                rowKey.ToBytes(),
                ColumnPredicateAll,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the sub columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(
            string columnFamily, string rowKey, byte[] superColName, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName),
                rowKey.ToBytes(),
                predicate,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(string columnFamily, byte[] rowKey, byte[] superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName), rowKey, ColumnPredicateAll, consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the sub columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(
            string columnFamily, byte[] rowKey, byte[] superColName, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily, superColName), rowKey, predicate, consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(string columnFamily, string rowKey, string superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName),
                rowKey.ToBytes(),
                ColumnPredicateAll,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the sub columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(
            string columnFamily, string rowKey, string superColName, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName),
                rowKey.ToBytes(),
                predicate,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(string columnFamily, byte[] rowKey, string superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily, superColName), rowKey, ColumnPredicateAll, consistencyLevel);
        }

        /// <summary>
        /// Get the count of sub-columns inside a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the sub columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the sub-columns
        /// </returns>
        public int GetSubColumnCount(
            string columnFamily, byte[] rowKey, string superColName, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily, superColName), rowKey, predicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a sub column from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column containing the sub column
        /// </param>
        /// <param name="subColName">
        /// The name of the sub column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetSubColumnFromRow(
            string columnFamily, string rowKey, byte[] superColName, string subColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSubColumnFromRow(
                columnFamily,
                rowKey.ToBytes(),
                superColName,
                subColName.ToBytes(),
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a sub column from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column containing the sub column
        /// </param>
        /// <param name="subColName">
        /// The name of the sub column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetSubColumnFromRow(
            string columnFamily, string rowKey, string superColName, string subColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSubColumnFromRow(
                columnFamily,
                rowKey.ToBytes(),
                superColName.ToBytes(),
                subColName.ToBytes(),
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a sub column from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column containing the sub column
        /// </param>
        /// <param name="subColName">
        /// The name of the sub column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetSubColumnFromRow(
            string columnFamily, string rowKey, string superColName, byte[] subColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSubColumnFromRow(
                columnFamily,
                rowKey.ToBytes(),
                superColName.ToBytes(),
                subColName,
                consistencyLevel);
        }

        /// <summary>
        /// Retrieve a sub column from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column containing the sub column
        /// </param>
        /// <param name="subColName">
        /// The name of the sub column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetSubColumnFromRow(
            string columnFamily, string rowKey, byte[] superColName, byte[] subColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSubColumnFromRow(
                columnFamily, rowKey.ToBytes(), superColName, subColName, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a sub column from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column containing the sub column
        /// </param>
        /// <param name="subColName">
        /// The name of the sub column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// Column
        /// </code>
        /// </returns>
        public Column GetSubColumnFromRow(
            string columnFamily, byte[] rowKey, byte[] superColName, byte[] subColName, ConsistencyLevel consistencyLevel)
        {
            ColumnPath cp = NewColumnPath(columnFamily, superColName, subColName);
            var operation = new ExecutionBlock<Column>(
                delegate(Cassandra.Iface myclient)
                {
                    ColumnOrSuperColumn cosc = myclient.get(Validation.safeGetRowKey(rowKey), cp, consistencyLevel);
                    return cosc.Column;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily, byte[] rowKey, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(
                NewColumnParent(columnFamily, superColName), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily, superColName), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily, string rowKey, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(
                NewColumnParent(columnFamily, superColName), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily,
            string rowKey,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily, superColName), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily, string rowKey, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(
                NewColumnParent(columnFamily, superColName), rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super column
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<Column> GetSubColumnsFromRow(
            string columnFamily,
            string rowKey,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(NewColumnParent(columnFamily, superColName), rowKey, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a set of rows.
        ///   Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map (LinkedHashMap) from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily,
            List<byte[]> rowKeys,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily,
            List<byte[]> rowKeys,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily, KeyRange keyRange, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily, superColName), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily,
            KeyRange keyRange,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily, superColName), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily, KeyRange keyRange, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(
                NewColumnParent(columnFamily, superColName), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<byte[], List<Column>>> GetSubColumnsFromRows(
            string columnFamily,
            KeyRange keyRange,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRows(NewColumnParent(columnFamily, superColName), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map but with an empty list as it's value.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily,
            List<string> rowKeys,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns.  If no value corresponding to a key is present, the key will still be in the map.
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a set of rows.
        /// </summary>
        /// <summary>
        /// Note that the returned map is insertion-order-preserving and populated based on the provided list of rowKeys.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily,
            List<string> rowKeys,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily,
            KeyRange keyRange,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending sub-column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve sub-columns from a super column in a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of sub-columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="superColName">
        /// The name of the super column
        /// </param>
        /// <param name="colPredicate">
        /// The sub-column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of sub-columns
        /// </returns>
        public List<KeyValuePair<string, List<Column>>> GetSubColumnsFromRowsUtf8Keys(
            string columnFamily,
            KeyRange keyRange,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRowsUtf8Keys(
                NewColumnParent(columnFamily, superColName), keyRange, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// The get sub counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <param name="reversed">
        /// The reversed.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// returns sub counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetSubCounterColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, string superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// The get sub counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// returns sub counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetSubCounterColumnsFromRows(
            string columnFamily,
            List<byte[]> rowKeys,
            string superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// The get sub counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <param name="reversed">
        /// The reversed.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// returns sub counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetSubCounterColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, byte[] superColName, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// The get sub counter columns from rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// sub counter columns from rows.
        /// </returns>
        public List<KeyValuePair<byte[], List<CounterColumn>>> GetSubCounterColumnsFromRows(
            string columnFamily,
            List<byte[]> rowKeys,
            byte[] superColName,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return this.GetCounterColumnsFromRows(
                NewColumnParent(columnFamily, superColName), rowKeys, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Get the count of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the super columns
        /// </returns>
        public int GetSuperColumnCount(string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily), rowKey, ColumnPredicateAll, consistencyLevel);
        }

        /// <summary>
        /// Get the count of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the super columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the super columns
        /// </returns>
        public int GetSuperColumnCount(
            string columnFamily, byte[] rowKey, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(NewColumnParent(columnFamily), rowKey, predicate, consistencyLevel);
        }

        /// <summary>
        /// Get the count of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the super columns
        /// </returns>
        public int GetSuperColumnCount(string columnFamily, string rowKey, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily),
                rowKey.ToBytes(),
                ColumnPredicateAll,
                consistencyLevel);
        }

        /// <summary>
        /// Get the count of super columns in a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="predicate">
        /// A predicate selecting the super columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The count of the super columns
        /// </returns>
        public int GetSuperColumnCount(
            string columnFamily, string rowKey, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnCount(
                NewColumnParent(columnFamily), rowKey.ToBytes(), predicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a super column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// SuperColumn
        /// </code>
        /// </returns>
        public SuperColumn GetSuperColumnFromRow(
            string columnFamily, string rowKey, string superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnFromRow(
                columnFamily, rowKey, superColName.ToBytes(), consistencyLevel);
        }

        /// <summary>
        /// Retrieve a super column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// SuperColumn
        /// </code>
        /// </returns>
        public SuperColumn GetSuperColumnFromRow(
            string columnFamily, string rowKey, byte[] superColName, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnFromRow(
                columnFamily, rowKey.ToBytes(), superColName, consistencyLevel);
        }

        /// <summary>
        /// Retrieve a super column from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="superColName">
        /// The name of the super column to retrieve
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The requested 
        /// <code>
        /// SuperColumn
        /// </code>
        /// </returns>
        public SuperColumn GetSuperColumnFromRow(
            string columnFamily, byte[] rowKey, byte[] superColName, ConsistencyLevel consistencyLevel)
        {
            ColumnPath cp = NewColumnPath(columnFamily, superColName, null);
            var operation = new ExecutionBlock<SuperColumn>(
                delegate(Cassandra.Iface myclient)
                {
                    ColumnOrSuperColumn cosc = myclient.get(Validation.safeGetRowKey(rowKey), cp, consistencyLevel);
                    return cosc.Super_column;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all super columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<SuperColumn> GetSuperColumnsFromRow(
            string columnFamily, string rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRow(
                columnFamily, rowKey.ToBytes(), ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super columns
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<SuperColumn> GetSuperColumnsFromRow(
            string columnFamily, string rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRow(
                columnFamily, rowKey.ToBytes(), colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Retrieve all super columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<SuperColumn> GetSuperColumnsFromRow(
            string columnFamily, byte[] rowKey, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRow(columnFamily, rowKey, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a row.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the row
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the super columns
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A list of matching columns
        /// </returns>
        public List<SuperColumn> GetSuperColumnsFromRow(
            string columnFamily, byte[] rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            ColumnParent cp = NewColumnParent(columnFamily);
            var operation = new ExecutionBlock<List<SuperColumn>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<ColumnOrSuperColumn> apiResult = myclient.get_slice(
                        Validation.safeGetRowKey(rowKey), cp, colPredicate, consistencyLevel);
                    return ToSuperColumnList(apiResult);
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all super columns from a set of rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<byte[], List<SuperColumn>>> GetSuperColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRows(columnFamily, rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a set of rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<byte[], List<SuperColumn>>> GetSuperColumnsFromRows(
            string columnFamily, List<byte[]> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var cp = NewColumnParent(columnFamily);
            var keys = Validation.validateRowKeys(rowKeys);

            var operation =
                new ExecutionBlock<List<KeyValuePair<byte[], List<SuperColumn>>>>(
                    delegate(Cassandra.Iface myclient)
                    {
                        var apiResult = myclient.multiget_slice(
                            keys, cp, colPredicate, consistencyLevel);
                        var result = new List<KeyValuePair<byte[], List<SuperColumn>>>(apiResult.Count);

                        result.AddRange(
                            from rowKey in rowKeys
                            let coscList = apiResult[rowKey]
                            let columns = ToSuperColumnList(coscList)
                            select new KeyValuePair<byte[], List<SuperColumn>>(rowKey, columns));

                        return result;
                    });

            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all super columns from a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of super columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<byte[], List<SuperColumn>>> GetSuperColumnsFromRows(
            string columnFamily, KeyRange keyRange, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRows(columnFamily, keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of super columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<byte[], List<SuperColumn>>> GetSuperColumnsFromRows(
            string columnFamily, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var cp = NewColumnParent(columnFamily);

            var operation =
                new ExecutionBlock<List<KeyValuePair<byte[], List<SuperColumn>>>>(
                    delegate(Cassandra.Iface myclient)
                    {
                        var apiResult = myclient.get_range_slices(cp, colPredicate, keyRange, consistencyLevel);
                        var result = new List<KeyValuePair<byte[], List<SuperColumn>>>(apiResult.Count);
                        result.AddRange(
                            from ks in apiResult
                            let coscList = ks.Columns
                            let colList = ToSuperColumnList(coscList)
                            select new KeyValuePair<byte[], List<SuperColumn>>(ks.Key, colList));

                        return result;
                    });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all super columns from a set of rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<string, List<SuperColumn>>> GetSuperColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRowsUtf8Keys(columnFamily, rowKeys, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a set of rows.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="rowKeys">
        /// The keys of the rows containing the super columns
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<string, List<SuperColumn>>> GetSuperColumnsFromRowsUtf8Keys(
            string columnFamily, List<string> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            ColumnParent cp = NewColumnParent(columnFamily);
            var keys = new List<byte[]>();
            Validation.validateRowKeysUtf8(rowKeys).ForEach(o => keys.Add(o.ToBytes()));
            var operation =
                new ExecutionBlock<List<KeyValuePair<string, List<SuperColumn>>>>(
                    delegate(Cassandra.Iface myclient)
                    {
                        var apiResult = myclient.multiget_slice(
                            keys, cp, colPredicate, consistencyLevel);
                        var result = new List<KeyValuePair<string, List<SuperColumn>>>(apiResult.Count);
                        result.AddRange(apiResult.Select(aResult =>
                            new KeyValuePair<string, List<SuperColumn>>(aResult.Key.ToUtf8String(), ToSuperColumnList(aResult.Value))));

                        return result;
                    });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Retrieve all super columns from a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of super columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="reversed">
        /// Whether the results should be returned in descending super column name order
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<string, List<SuperColumn>>> GetSuperColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, bool reversed, ConsistencyLevel consistencyLevel)
        {
            return this.GetSuperColumnsFromRowsUtf8Keys(columnFamily, keyRange, ColumnsPredicateAll(reversed), consistencyLevel);
        }

        /// <summary>
        /// Retrieve super columns from a range of rows.
        /// </summary>
        /// <summary>
        /// The method returns a map from the keys of rows in the specified range to lists of super columns from the rows. The map
        /// </summary>
        /// <summary>
        /// returned is a 
        /// <code>
        /// LinkedHashMap
        /// </code>
        /// and its key iterator proceeds in the order that the key data was returned by
        /// </summary>
        /// <summary>
        /// Cassandra. If the cluster uses the RandomPartitioner, this order appears random.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family containing the rows
        /// </param>
        /// <param name="keyRange">
        /// A key range selecting the rows
        /// </param>
        /// <param name="colPredicate">
        /// The super column selector predicate
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A map from row keys to the matching lists of super columns
        /// </returns>
        public List<KeyValuePair<string, List<SuperColumn>>> GetSuperColumnsFromRowsUtf8Keys(
            string columnFamily, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            ColumnParent cp = NewColumnParent(columnFamily);
            var operation =
                new ExecutionBlock<List<KeyValuePair<string, List<SuperColumn>>>>(
                    delegate(Cassandra.Iface myclient)
                    {
                        List<KeySlice> apiResult = myclient.get_range_slices(cp, colPredicate, keyRange, consistencyLevel);
                        var result = new List<KeyValuePair<string, List<SuperColumn>>>();
                        foreach (KeySlice ks in apiResult)
                        {
                            List<ColumnOrSuperColumn> coscList = ks.Columns;
                            List<SuperColumn> colList = ToSuperColumnList(coscList);
                            result.Add(new KeyValuePair<string, List<SuperColumn>>(ks.Key.ToUtf8String(), colList));
                        }

                        return result;
                    });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Executa uma query cql qualquer no cassandra
        /// </summary>
        /// <param name="query">query a se executada</param>
        /// <returns>Resultado da query</returns>
        public CqlResult ExecuteCQL(string query)
        {
            var operation = new ExecutionBlock<CqlResult>(myclient => myclient.execute_cql_query(query.ToBytes(), Compression.NONE));
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// Realiza uma busca
        /// </summary>
        /// <param name="columns">
        /// Colunas a serem retornadas na busca, separadas por ','
        /// </param>
        /// <param name="columnFamily">
        /// Column Family usada na busca
        /// </param>
        /// <param name="query">
        /// Query compatível com o Solr/Lucene
        /// </param>
        /// <param name="maxResults">
        /// Número máximo de resultados
        /// </param>
        /// <param name="consistencyLevel">
        /// The consistency level.
        /// </param>
        /// <returns>
        /// Registros encontrados na busca
        /// </returns>
        public List<CqlRow> Search(string columns, string columnFamily, string query, int maxResults, ConsistencyLevel consistencyLevel)
        {
            var cqlQuery = "SELECT " + columns + " FROM " + columnFamily + " USING Consistency " + consistencyLevel
                           + " WHERE solr_query = '" + query + "' LIMIT " + maxResults;
            var cqlResult = this.ExecuteCQL(cqlQuery);
            return cqlResult.Rows;
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over columns.  The returned iterator delegates to
        ///   fetch batches of columns (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest  column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of columns that can be retrieved per invocation to {@link #getPageOfColumnsFromRow(string, string, byte[], bool, int, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of columns to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// An iterator of columns
        /// </returns>
        public IIterator<Column> IterateColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] startBeyondName,
            bool reversed,
            int batchSize,
            ConsistencyLevel consistencyLevel)
        {
            return new ColumnIterator(this, columnFamily, rowKey, startBeyondName, reversed, batchSize, consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over columns.  The returned iterator delegates fetch batches of columns (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of columns must begin with the smallest  column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending column name order
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of columns that can be retrieved per invocation to {@link #getPageOfColumnsFromRow(string, string, byte[], bool, int, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of columns to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// An iterator of columns
        /// </returns>
        public IIterator<Column> IterateColumnsFromRow(
            string columnFamily,
            string rowKey,
            string startBeyondName,
            bool reversed,
            int batchSize,
            ConsistencyLevel consistencyLevel)
        {
            return this.IterateColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName.ToBytes(),
                reversed,
                batchSize,
                consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over rows.  The returned iterator delegates to
        /// </summary>
        /// <summary>
        /// to fetch batches of rows (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the columns
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of columns that can be retrieved per invocation to {@link #getColumnsFromRows(java.lang.string, java.util.List, bool, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of rows to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// An iterator of columns
        /// </returns>
        public IIterator<KeyValuePair<byte[], List<Column>>> IterateColumnsFromRows(
            string columnFamily, int batchSize, ConsistencyLevel consistencyLevel)
        {
            return this.IterateColumnsFromRows(
                columnFamily, new byte[0], batchSize, NewColumnsPredicateAll(false), consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over rows.  The returned iterator delegates to fetch batches of rows (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the columns
        /// </param>
        /// <param name="startBeyondKey">
        /// The sequence of row keys must begin with the smallest row key greater than this value. Pass 
        /// to start at the beginning of the sequence.  NOTE: this parameter only really makes sense when using an Order Preserving Partishioner.
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of columns that can be retrieved per invocation to {@link #getColumnsFromRows(java.lang.string, java.util.List, bool, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of rows to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// An iterator of columns
        /// </returns>
        public IIterator<KeyValuePair<byte[], List<Column>>> IterateColumnsFromRows(
            string columnFamily, byte[] startBeyondKey, int batchSize, ConsistencyLevel consistencyLevel)
        {
            return this.IterateColumnsFromRows(
                columnFamily, startBeyondKey, batchSize, NewColumnsPredicateAll(false), consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over rows.  The returned iterator delegates to fetch batches of rows (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the columns
        /// </param>
        /// <param name="startBeyondKey">
        /// The sequence of row keys must begin with the smallest row key greater than this value. Pass 
        /// <code>
        /// {@link byte[]#EMPTY}
        /// </code>
        /// to start at the beginning of the sequence.  NOTE: this parameter only really makes sense when using an Order Preserving Partishioner.
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of columns that can be retrieved per invocation to {@link #getColumnsFromRows(java.lang.string, java.util.List, bool, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of rows to be held in memory at any one time
        /// </param>
        /// <param name="colPredicate">
        /// Dictates the columns to include
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// An iterator of columns
        /// </returns>
        public IIterator<KeyValuePair<byte[], List<Column>>> IterateColumnsFromRows(
            string columnFamily,
            byte[] startBeyondKey,
            int batchSize,
            SlicePredicate colPredicate,
            ConsistencyLevel consistencyLevel)
        {
            return new ColumnRowIterator(this, columnFamily, startBeyondKey, batchSize, colPredicate, consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over super columns.  The returned iterator delegates to fetch batches of super columns (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of super columns that can be retrieved per invocation to {@link #getPageOfSuperColumnsFromRow(string, string, byte[], bool, int, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of super columns to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public IIterator<SuperColumn> IterateSuperColumnsFromRow(
            string columnFamily,
            byte[] rowKey,
            byte[] startBeyondName,
            bool reversed,
            int batchSize,
            ConsistencyLevel consistencyLevel)
        {
            return new SuperColumnIterator(this, columnFamily, rowKey, startBeyondName, reversed, batchSize, consistencyLevel);
        }

        /// <summary>
        /// Returns an iterator that can be used to iterate over super columns.  The returned iterator delegates to fetch batches of super columns (based on the batchSize parameter).
        /// </summary>
        /// <param name="columnFamily">
        /// The name of the column family containing the super columns
        /// </param>
        /// <param name="rowKey">
        /// The key of the row
        /// </param>
        /// <param name="startBeyondName">
        /// The sequence of super columns must begin with the smallest super column name greater than this value. Pass 
        /// <code>
        /// null
        /// </code>
        /// to start at the beginning of the sequence.
        /// </param>
        /// <param name="reversed">
        /// Whether the scan should proceed in descending super column name order
        /// </param>
        /// <param name="batchSize">
        /// The maximum number of super columns that can be retrieved per invocation to {@link #getPageOfSuperColumnsFromRow(string, string, byte[], bool, int, org.apache.cassandra.thrift.ConsistencyLevel)} and dictates the number of super columns to be held in memory at any one time
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// A page of super columns
        /// </returns>
        public IIterator<SuperColumn> IterateSuperColumnsFromRow(
            string columnFamily,
            string rowKey,
            string startBeyondName,
            bool reversed,
            int batchSize,
            ConsistencyLevel consistencyLevel)
        {
            return this.IterateSuperColumnsFromRow(
                columnFamily,
                rowKey.ToBytes(),
                startBeyondName.ToBytes(),
                reversed,
                batchSize,
                consistencyLevel);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Create the internal 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// instance that selects "all" columns with no imposed limit.
        /// </summary>
        /// <summary>
        /// Note: these instances should be handled carefully, as they are mutable.
        /// </summary>
        /// <param name="reversed">
        /// Whether the results should be returned in reverse order
        /// </param>
        /// <returns>
        /// The new 
        /// <code>
        /// SlicePredicate
        /// </code>
        /// </returns>
        private static SlicePredicate ColumnsPredicateAll(bool reversed)
        {
            return reversed ? ColumnsPredicateAllReversed : ColumnPredicateAll;
        }

        /// <summary>
        /// The new column parent.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <returns>
        /// Returns the column parent
        /// </returns>
        private static ColumnParent NewColumnParent(string columnFamily, string superColName)
        {
            return NewColumnParent(columnFamily, superColName.ToBytes());
        }

        /// <summary>
        /// The new column parent.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <returns>
        /// Returns the Column Parent
        /// </returns>
        private static ColumnParent NewColumnParent(string columnFamily, byte[] superColName)
        {
            var parent = new ColumnParent { Column_family = columnFamily, Super_column = superColName };
            return parent;
        }

        /// <summary>
        /// The new column path.
        /// </summary>
        /// <param name="columnFamily">
        /// The column family.
        /// </param>
        /// <param name="superColName">
        /// The super col name.
        /// </param>
        /// <param name="colName">
        /// The col name.
        /// </param>
        /// <returns>
        /// retrusn the columnPath
        /// </returns>
        private static ColumnPath NewColumnPath(string columnFamily, byte[] superColName, byte[] colName)
        {
            var path = new ColumnPath { Column_family = columnFamily, Super_column = superColName, Column = colName };
            return path;
        }

        /// <summary>
        /// The to column list.
        /// </summary>
        /// <param name="coscList">
        /// The cosc list.
        /// </param>
        /// <returns>
        /// Convert ColumnOrSuperColumnList to ColumnList
        /// </returns>
        private static List<Column> ToColumnList(IList<ColumnOrSuperColumn> coscList)
        {
            var columns = new List<Column>(coscList.Count());
            foreach (var cosc in coscList)
            {
                if (cosc.Column == null)
                {
                    continue;
                    throw new ArgumentException("The column should not be null");
                }

                columns.Add(cosc.Column);
            }

            return columns;
        }

        /// <summary>
        /// The to counter column list.
        /// </summary>
        /// <param name="coscList">
        /// The cosc list.
        /// </param>
        /// <returns>
        /// Convert ColumnOrSuperColumnList to CounterColumnList
        /// </returns>
        private static List<CounterColumn> ToCounterColumnList(IList<ColumnOrSuperColumn> coscList)
        {
            var columns = new List<CounterColumn>(coscList.Count());
            foreach (var cosc in coscList)
            {
                if (cosc.Counter_column == null)
                {
                    throw new ArgumentException("The column should not be null");
                }

                columns.Add(cosc.Counter_column);
            }

            return columns;
        }

        /// <summary>
        /// The to super column list.
        /// </summary>
        /// <param name="coscList">
        /// The cosc list.
        /// </param>
        /// <returns>
        /// Convert ColumnOrSuperColumnList to SuperColumnList
        /// </returns>
        private static List<SuperColumn> ToSuperColumnList(IList<ColumnOrSuperColumn> coscList)
        {
            var columns = new List<SuperColumn>(coscList.Count);
            foreach (ColumnOrSuperColumn cosc in coscList)
            {
                if (cosc.Super_column == null)
                {
                    throw new ArgumentException("The super column should not be null");
                }

                columns.Add(cosc.Super_column);
            }

            return columns;
        }

        /// <summary>
        /// Get the count of columns in a row with a matching predicate
        /// </summary>
        /// <param name="colParent">
        /// The parent of the columns to be counted
        /// </param>
        /// <param name="rowKey">
        /// The key of the row containing the columns
        /// </param>
        /// <param name="predicate">
        /// The slice predicate selecting the columns to be counted
        /// </param>
        /// <param name="consistencyLevel">
        /// The Cassandra consistency level with which to perform the operation
        /// </param>
        /// <returns>
        /// The number of matching columns
        /// </returns>
        private int GetColumnCount(
            ColumnParent colParent, byte[] rowKey, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
        {
            var operation =
                new ExecutionBlock<int>(
                    myclient => myclient.get_count(Validation.safeGetRowKey(rowKey), colParent, predicate, consistencyLevel));

            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from row.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<Column> GetColumnsFromRow(
            ColumnParent colParent, string rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            return this.GetColumnsFromRow(
                colParent, rowKey.ToBytes(), colPredicate, consistencyLevel);
        }

        /// <summary>
        /// The get columns from row.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<Column> GetColumnsFromRow(
            ColumnParent colParent, byte[] rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<Column>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<ColumnOrSuperColumn> apiResult = myclient.get_slice(
                        Validation.safeGetRowKey(rowKey), colParent, colPredicate, consistencyLevel);
                    return ToColumnList(apiResult);
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from rows.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            ColumnParent colParent, List<byte[]> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            List<byte[]> keys = Validation.validateRowKeys(rowKeys);
            var operation = new ExecutionBlock<List<KeyValuePair<byte[], List<Column>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    var apiResult = myclient.multiget_slice(
                        keys, colParent, colPredicate, consistencyLevel);

                    return apiResult.Select(keyValuePair =>
                        new KeyValuePair<byte[], List<Column>>(keyValuePair.Key, ToColumnList(keyValuePair.Value))).ToList();
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from rows.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="keyRange">
        /// The key range.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns the columns from row
        /// </returns>
        private List<KeyValuePair<byte[], List<Column>>> GetColumnsFromRows(
            ColumnParent colParent, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<KeyValuePair<byte[], List<Column>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<KeySlice> apiResult = myclient.get_range_slices(colParent, colPredicate, keyRange, consistencyLevel);
                    var result = new List<KeyValuePair<byte[], List<Column>>>();
                    foreach (KeySlice ks in apiResult)
                    {
                        List<ColumnOrSuperColumn> coscList = ks.Columns;
                        List<Column> colList = ToColumnList(coscList);
                        result.Add(new KeyValuePair<byte[], List<Column>>(ks.Key, colList));
                    }

                    return result;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from rows.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="keyRange">
        /// The key range.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns the columns from row
        /// </returns>
        private List<KeyValuePair<byte[], List<CounterColumn>>> GetCounterColumnsFromRows(
            ColumnParent colParent, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<KeyValuePair<byte[], List<CounterColumn>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<KeySlice> apiResult = myclient.get_range_slices(colParent, colPredicate, keyRange, consistencyLevel);
                    var result = new List<KeyValuePair<byte[], List<CounterColumn>>>(apiResult.Count);
                    result.AddRange(
                        from ks in apiResult
                        let coscList = ks.Columns
                        let colList = ToCounterColumnList(coscList)
                        select new KeyValuePair<byte[], List<CounterColumn>>(ks.Key, colList));

                    return result;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from rows utf 8 keys.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            ColumnParent colParent, List<string> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var keys = new List<byte[]>();
            Validation.validateRowKeysUtf8(rowKeys).ForEach(o => keys.Add(o.ToBytes()));

            var operation = new ExecutionBlock<List<KeyValuePair<string, List<Column>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    var apiResult = myclient.multiget_slice(
                        keys, colParent, colPredicate, consistencyLevel);
                    var result = new List<KeyValuePair<string, List<Column>>>(apiResult.Count);

                    result.AddRange(apiResult.Select(aResult =>
                        new KeyValuePair<string, List<Column>>(aResult.Key.ToUtf8String(), ToColumnList(aResult.Value))));

                    return result;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get columns from rows utf 8 keys.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="keyRange">
        /// The key range.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<KeyValuePair<string, List<Column>>> GetColumnsFromRowsUtf8Keys(
            ColumnParent colParent, KeyRange keyRange, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<KeyValuePair<string, List<Column>>>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<KeySlice> apiResult = myclient.get_range_slices(colParent, colPredicate, keyRange, consistencyLevel);
                    var result = new List<KeyValuePair<string, List<Column>>>(apiResult.Count);
                    result.AddRange(
                        from ks in apiResult
                        let coscList = ks.Columns
                        let colList = ToColumnList(coscList)
                        select new KeyValuePair<string, List<Column>>(ks.Key.ToUtf8String(), colList));

                    return result;
                });
            return this.cluster.Execute(operation, this.keyspace);
        }


        /// <summary>
        /// The get counter columns from row.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKey">
        /// The row key.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns columns from the row identified with rowkey.
        /// </returns>
        private List<CounterColumn> GetCounterColumnsFromRow(
            ColumnParent colParent, byte[] rowKey, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            var operation = new ExecutionBlock<List<CounterColumn>>(
                delegate(Cassandra.Iface myclient)
                {
                    List<ColumnOrSuperColumn> apiResult = myclient.get_slice(
                        Validation.safeGetRowKey(rowKey), colParent, colPredicate, consistencyLevel);
                    return ToCounterColumnList(apiResult);
                });
            return this.cluster.Execute(operation, this.keyspace);
        }

        /// <summary>
        /// The get counter columns from rows.
        /// </summary>
        /// <param name="colParent">
        /// The col parent.
        /// </param>
        /// <param name="rowKeys">
        /// The row keys.
        /// </param>
        /// <param name="colPredicate">
        /// The col predicate.
        /// </param>
        /// <param name="consistencyLevel">
        /// The c level.
        /// </param>
        /// <returns>
        /// Returns counter columns from the row identified with rowkey.
        /// </returns>
        private List<KeyValuePair<byte[], List<CounterColumn>>> GetCounterColumnsFromRows(
            ColumnParent colParent, List<byte[]> rowKeys, SlicePredicate colPredicate, ConsistencyLevel consistencyLevel)
        {
            List<byte[]> keys = Validation.validateRowKeys(rowKeys);
            var operation =
                new ExecutionBlock<List<KeyValuePair<byte[], List<CounterColumn>>>>(
                    delegate(Cassandra.Iface myclient)
                    {
                        var apiResult = myclient.multiget_slice(
                            keys, colParent, colPredicate, consistencyLevel);
                        var result = new List<KeyValuePair<byte[], List<CounterColumn>>>(apiResult.Count);

                        result.AddRange(
                            from aResult in apiResult
                            let columns = ToCounterColumnList(aResult.Value)
                            select new KeyValuePair<byte[], List<CounterColumn>>(aResult.Key, columns));

                        return result;
                    });
            return this.cluster.Execute(operation, this.keyspace);
        }

        #endregion
    }
}