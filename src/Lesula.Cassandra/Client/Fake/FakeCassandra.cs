namespace Lesula.Cassandra.Client.Fake
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    using Apache.Cassandra;

    /// <summary>
    /// A Fake cassandra client used for mocks and unit tests
    /// </summary>
    /// <remarks>
    /// Inspired from a python github gist
    /// https://gist.github.com/376249
    /// </remarks>
    public class FakeCassandra : Cassandra.Iface
    {
        private static Dictionary<string, FakeKeyspace> database;
        private static Dictionary<string, List<string>> keyspaces;
        private static string currentKeyspace;
        private static long version = 1;

        static FakeCassandra()
        {
            Init();
        }

        public static void Init()
        {
            database = new Dictionary<string, FakeKeyspace>();
            keyspaces = new Dictionary<string, List<string>>();
            currentKeyspace = string.Empty;
            version = 1;
        }

        /// <summary>
        /// Authenticates with the cluster using the specified AuthenticationRequest credentials.
        /// Throws AuthenticationException if the credentials are invalid or AuthorizationException if the credentials are valid,
        /// but not for the specified keyspace.
        /// </summary>
        /// <param name="auth_request">AuthenticationRequest credentials</param>
        public void login(AuthenticationRequest auth_request)
        {
            // NoOp
        }

        /// <summary>
        /// Set the keyspace to use for subsequent requests. Throws InvalidRequestException for an unknown keyspace.
        /// </summary>
        /// <param name="keyspace">requested keyspace</param>
        public void set_keyspace(string keyspace)
        {
            if (!keyspaces.ContainsKey(keyspace))
            {
                throw new InvalidRequestException();
            }

            currentKeyspace = keyspace;
        }

        /// <summary>
        /// Get the Column or SuperColumn at the given column_path.
        ///  If no value is present, NotFoundException is thrown. 
        /// (This is the only method that can throw an exception under non-failure conditions.)
        /// </summary>
        /// <param name="key">row key</param>
        /// <param name="column_path">the column path</param>
        /// <param name="consistency_level">the consistency level</param>
        /// <returns>desired column or supercolumn</returns>
        public ColumnOrSuperColumn get(byte[] key, ColumnPath column_path, ConsistencyLevel consistency_level)
        {
            if (!database[currentKeyspace].ContainsKey(column_path.Column_family))
            {
                throw new Exception("Column family not found");
            }

            if (!database[currentKeyspace][column_path.Column_family].Contains(key))
            {
                throw new NotFoundException();
            }

            if (!database[currentKeyspace][column_path.Column_family][key].ContainsKey(column_path.Column))
            {
                throw new NotFoundException();
            }

            return database[currentKeyspace][column_path.Column_family][key][column_path.Column];
        }

        /// <summary>
        /// Get the group of columns contained by column_parent (either a ColumnFamily name or a ColumnFamily/SuperColumn name pair)
        /// specified by the given SlicePredicate struct.
        /// </summary>
        public List<ColumnOrSuperColumn> get_slice(byte[] key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
        {
            if (!database[currentKeyspace].ContainsKey(column_parent.Column_family))
            {
                throw new Exception("Column family not found");
            }

            if (!database[currentKeyspace][column_parent.Column_family].Contains(key))
            {
                return new List<ColumnOrSuperColumn>();
            }

            var row = database[currentKeyspace][column_parent.Column_family][key];
            if (predicate.__isset.column_names)
            {
                var result = predicate.Column_names.Select(colName => row[colName]).ToList();
                return result;
            }

            if (predicate.__isset.slice_range)
            {
                var names = new SortedSet<byte[]>(new ByteArrayComparer());
                foreach (var columnName in row.Keys)
                {
                    names.Add(columnName);
                }

                var view = FakeSSTable.GetView(
                    names,
                    predicate.Slice_range.Start,
                    predicate.Slice_range.Finish,
                    predicate.Slice_range.Count,
                    predicate.Slice_range.Reversed);

                return view.Select(name => row[name]).ToList();
            }

            throw new Exception("Neither slice_range or column_names are set.");
        }

        /// <summary>
        /// Counts the columns present in column_parent within the predicate.
        /// The method is not O(1). It takes all the columns from disk to calculate the answer.
        /// The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them.
        /// </summary>
        public int get_count(byte[] key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
        {
            return this.get_slice(key, column_parent, predicate, consistency_level).Count;
        }

        /// <summary>
        /// Retrieves slices for column_parent and predicate on each of the given keys in parallel. Keys are a `list&lt;string&gt; of the keys 
        /// to get slices for.
        /// This is similar to get_range_slices, except it operates on a set of non-contiguous keys instead of a range of keys.
        /// </summary>
        public Dictionary<byte[], List<ColumnOrSuperColumn>> multiget_slice(List<byte[]> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
        {
            var result = new Dictionary<byte[], List<ColumnOrSuperColumn>>(new ByteArrayComparer());
            foreach (var key in keys)
            {
                result[key] = this.get_slice(key, column_parent, predicate, consistency_level);
            }

            return result;
        }

        /// <summary>
        /// A combination of multiget_slice and get_count.
        /// </summary>
        public Dictionary<byte[], int> multiget_count(List<byte[]> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
        {
            var result = new Dictionary<byte[], int>(new ByteArrayComparer());
            foreach (var key in keys)
            {
                result[key] = this.get_count(key, column_parent, predicate, consistency_level);
            }

            return result;
        }

        /// <summary>
        /// Replaces get_range_slice. Returns a list of slices for the keys within the specified KeyRange.
        /// Unlike get_key_range, this applies the given predicate to all keys in the range, not just those with undeleted matching data.
        /// Note that when using RandomPartitioner, keys are stored in the order of their MD5 hash, making it impossible to get a meaningful
        /// range of keys between two endpoints.
        /// </summary>
        public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
        {
            if (!database[currentKeyspace].ContainsKey(column_parent.Column_family))
            {
                throw new Exception("Column family not found");
            }

            var filteredKeys = database[currentKeyspace][column_parent.Column_family].KeySlice(range);
            var resultDic = this.multiget_slice(filteredKeys, column_parent, predicate, consistency_level);
            var result = resultDic.Select(entry => new KeySlice { Columns = entry.Value, Key = entry.Key }).ToList();
            return result;
        }

        public List<KeySlice> get_paged_slice(string column_family, KeyRange range, byte[] start_column, ConsistencyLevel consistency_level)
        {
            throw new NotImplementedException("Undocumented api call. See: http://stackoverflow.com/a/13357749/1020222");
        }

        /// <summary>
        /// Like get_range_slices, returns a list of slices, but uses IndexClause instead of KeyRange.
        /// To use this method, the underlying ColumnFamily of the ColumnParent must have been configured with a column_metadata attribute,
        /// specifying at least the name and index_type attributes.
        /// See CfDef and ColumnDef above for the list of attributes.
        /// Note: the IndexClause must contain one IndexExpression with an EQ operator on a configured index column.
        /// Other IndexExpression structs may be added to the IndexClause for non-indexed columns to further refine the results of the EQ expression.
        /// </summary>
        public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
        {
            var keyRange = new KeyRange
                {
                    Start_key = index_clause.Start_key,
                    Row_filter = index_clause.Expressions,
                    Count = index_clause.Count
                };

            return this.get_range_slices(column_parent, column_predicate, keyRange, consistency_level);
        }

        /// <summary>
        /// Insert a Column consisting of (name, value, timestamp, ttl) at the given ColumnParent. 
        /// Note that a SuperColumn cannot directly contain binary values -- it can only contain sub-Columns.
        /// Only one sub-Column may be inserted at a time, as well.
        /// </summary>
        public void insert(byte[] key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
        {
            try
            {
                var path = new ColumnPath { Column = column.Name, Column_family = column_parent.Column_family };
                var current = this.get(key, path, consistency_level);
                if (column.Timestamp > current.Column.Timestamp)
                {
                    current.Column.Value = column.Value;
                }
            }
            catch (NotFoundException)
            {
                this.CheckKeyspace();
                var cosc = new ColumnOrSuperColumn { Column = column };
                if (!database[currentKeyspace][column_parent.Column_family].Contains(key))
                {
                    database[currentKeyspace][column_parent.Column_family][key] = new FakeRow(key);
                }

                database[currentKeyspace][column_parent.Column_family][key][column.Name] = cosc;
            }
        }

        private void CheckKeyspace()
        {
            if (!database.ContainsKey(currentKeyspace))
            {
                var newKeyspace = new FakeKeyspace();
                foreach (var cfname in keyspaces[currentKeyspace])
                {
                    newKeyspace.Add(cfname, new FakeSSTable());
                }

                database[currentKeyspace] = newKeyspace;
            }
        }

        /// <summary>
        /// Increments a CounterColumn consisting of (name, value) at the given ColumnParent.
        /// Note that a SuperColumn cannot directly contain binary values -- it can only contain sub-Columns.
        /// </summary>
        public void add(byte[] key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level)
        {
            try
            {
                var path = new ColumnPath { Column = column.Name, Column_family = column_parent.Column_family };
                var current = this.get(key, path, consistency_level);

                if (!current.__isset.counter_column)
                {
                    throw new Exception("Not a counter column");
                }

                current.Counter_column.Value += column.Value;
            }
            catch (NotFoundException)
            {
                this.CheckKeyspace();
                var cosc = new ColumnOrSuperColumn { Counter_column = column };

                if (!database[currentKeyspace][column_parent.Column_family].Contains(key))
                {
                    database[currentKeyspace][column_parent.Column_family][key] = new FakeRow(key);
                }

                database[currentKeyspace][column_parent.Column_family][key][column.Name] = cosc;
            }
        }

        /// <summary>
        /// Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp.
        /// Note that all the values in column_path besides column_path.column_family are truly optional:
        /// you can remove the entire row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column 
        /// by specifying those levels too. Note that the timestamp is needed, so that if the commands are replayed in a different order on 
        /// different nodes, the same result is produced.
        /// </summary>
        public void remove(byte[] key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
        {
            if (!database.ContainsKey(currentKeyspace))
            {
                throw new Exception("Keyspace not found!");
            }

            if (!database[currentKeyspace].ContainsKey(column_path.Column_family))
            {
                throw new Exception("column family not found!");
            }

            if (!database[currentKeyspace][column_path.Column_family].Contains(key))
            {
                throw new Exception("row not found!");
            }

            if (column_path.__isset.column)
            {
                if (!database[currentKeyspace][column_path.Column_family][key].ContainsKey(column_path.Column))
                {
                   throw new Exception("column not found!");
                }

                database[currentKeyspace][column_path.Column_family][key].Remove(column_path.Column);
            }
            else
            {
                database[currentKeyspace][column_path.Column_family].DelItem(key);
            }
        }

        /// <summary>
        /// Remove a counter from the row specified by key at the granularity specified by column_path.
        /// Note that all the values in column_path besides column_path.column_family are truly optional:
        /// you can remove the entire row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column
        /// by specifying those levels too. Note that counters have limited support for deletes: if you remove a counter, you must wait to
        /// issue any following update until the delete has reached all the nodes and all of them have been fully compacted.
        /// </summary>
        public void remove_counter(byte[] key, ColumnPath path, ConsistencyLevel consistency_level)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Executes the specified mutations on the keyspace. mutation_map is a map&lt;string, map&lt;string, vector&lt;Mutation&gt;&gt;&gt;;
        /// the outer map maps the key to the inner map, which maps the column family to the Mutation; can be read as: 
        /// map&lt;key : string, map&lt;column_family : string, vector&lt;Mutation&gt;&gt;&gt;.
        /// To be more specific, the outer map key is a row key, the inner map key is the column family name.
        /// A Mutation specifies either columns to insert or columns to delete. See Mutation and Deletion above for more details.
        /// </summary>
        public void batch_mutate(Dictionary<byte[], Dictionary<string, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
        {
            foreach (var cfmap in mutation_map)
            {
                foreach (var mutations in cfmap.Value)
                {
                    foreach (var mutation in mutations.Value)
                    {
                        var parent = new ColumnParent { Column_family = mutations.Key };

                        if (mutation.__isset.deletion)
                        {
                            if (mutation.Deletion.Predicate.__isset.slice_range)
                            {
                                throw new Exception("Slice range on batch mutations not supported yet.");
                            }

                            foreach (var column in mutation.Deletion.Predicate.Column_names)
                            {
                                var path = new ColumnPath { Column = column, Column_family = mutations.Key };
                                this.remove(cfmap.Key, path, mutation.Deletion.Timestamp, consistency_level);
                            }
                        }
                        else if (mutation.Column_or_supercolumn.__isset.column)
                        {
                            var column = mutation.Column_or_supercolumn.Column;
                            this.insert(cfmap.Key, parent, column, consistency_level);
                        }
                        else if (mutation.Column_or_supercolumn.__isset.counter_column)
                        {
                            var counter = mutation.Column_or_supercolumn.Counter_column;
                            this.add(cfmap.Key, parent, counter, consistency_level);
                        }
                        else
                        {
                            throw new Exception("Supercolumns not supported yet.");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Removes all the rows from the given column family.
        /// </summary>
        public void truncate(string cfname)
        {
            if (!database[currentKeyspace].ContainsKey(cfname))
            {
                throw new Exception("Column family not found");
            }

            database[currentKeyspace][cfname] = new FakeSSTable { Definition = database[currentKeyspace][cfname].Definition };
        }

        /// <summary>
        /// For each schema version present in the cluster, returns a list of nodes at that version.
        /// Hosts that do not respond will be under the key DatabaseDescriptor.INITIAL_VERSION. 
        /// The cluster is all on the same version if the size of the map is 1.
        /// </summary>
        public Dictionary<string, List<string>> describe_schema_versions()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a list of all the keyspaces configured for the cluster. (Equivalent to calling describe_keyspace(k) for k in keyspaces.)
        /// </summary>
        public List<KsDef> describe_keyspaces()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the name of the cluster.
        /// </summary>
        public string describe_cluster_name()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the Thrift API version.
        /// </summary>
        public string describe_version()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the token ring; a map of ranges to host addresses.
        /// Represented as a set of TokenRange instead of a map from range to list of endpoints, because you can't use Thrift structs
        /// as map keys: https://issues.apache.org/jira/browse/THRIFT-162 for the same reason, we can't return a set here, even though
        /// order is neither important nor predictable.
        /// </summary>
        public List<TokenRange> describe_ring(string keyspace)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> describe_token_map()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the name of the partitioner for the cluster.
        /// </summary>
        public string describe_partitioner()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the name of the snitch used for the cluster.
        /// </summary>
        public string describe_snitch()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets information about the specified keyspace.
        /// </summary>
        public KsDef describe_keyspace(string keyspace)
        {
            if (!database.ContainsKey(keyspace))
            {
                throw new Exception("Keyspace not found!");
            }

            return database[keyspace].Definitions;
        }

        public List<string> describe_splits(string cfName, string start_token, string end_token, int keys_per_split)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Adds a column family. This method will throw an exception if a column family with the same name is already associated 
        /// with the keyspace.
        /// Returns the new schema version ID.
        /// </summary>
        public string system_add_column_family(CfDef cf_def)
        {
            if (!database.ContainsKey(cf_def.Keyspace))
            {
                throw new Exception("Keyspace not found!");
            }

            if (database[cf_def.Keyspace].ContainsKey(cf_def.Name))
            {
                throw new InvalidRequestException(){ Why = "Column Family Already Exists" };
            }

            database[cf_def.Keyspace][cf_def.Name] = new FakeSSTable { Definition = cf_def };

            if (database[cf_def.Keyspace].Definitions == null)
            {
                database[cf_def.Keyspace].Definitions = new KsDef();
            }

            if (!database[cf_def.Keyspace].Definitions.__isset.cf_defs)
            {
                database[cf_def.Keyspace].Definitions.Cf_defs = new List<CfDef>();
            }

            database[cf_def.Keyspace].Definitions.Cf_defs.Add(cf_def);
            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Drops a column family. Creates a snapshot and then submits a 'graveyard' compaction during which the abandoned files will be deleted.
        /// Returns the new schema version ID.
        /// </summary>
        public string system_drop_column_family(string column_family)
        {
            if (!database.ContainsKey(currentKeyspace))
            {
                throw new Exception("Keyspace not found!");
            }

            if (!database[currentKeyspace].ContainsKey(column_family))
            {
                throw new Exception("Column family not found!");
            }

            database[currentKeyspace].Remove(column_family);
            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Creates a new keyspace and any column families defined with it. 
        /// Callers are not required to first create an empty keyspace and then create column families for it. 
        /// Returns the new schema version ID.
        /// </summary>
        public string system_add_keyspace(KsDef ks_def)
        {
            if (database.ContainsKey(ks_def.Name))
            {
                throw new InvalidRequestException(){ Why = "Keyspace already exists." };
            }

            var ksName = ks_def.Name;
            database[ksName] = new FakeKeyspace { Definitions = ks_def };
            if (ks_def.Cf_defs != null)
            {
                var cfs = ks_def.Cf_defs.Select(c => c.Name).ToList();
                keyspaces[ksName] = cfs;
            }
            else
            {
                keyspaces[ksName] = new List<string>();
            }

            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Drops a keyspace. 
        /// Creates a snapshot and then submits a 'graveyard' compaction during which the abandoned files will be deleted. 
        /// Returns the new schema version ID.
        /// </summary>
        public string system_drop_keyspace(string keyspace)
        {
            if (!database.ContainsKey(keyspace))
            {
                throw new Exception("Keyspace not found!");
            }

            database.Remove(keyspace);
            keyspaces.Remove(keyspace);

            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Updates properties of a keyspace. returns the new schema id.
        /// </summary>
        public string system_update_keyspace(KsDef ks_def)
        {
            if (!database.ContainsKey(ks_def.Name))
            {
                throw new Exception("Keyspace not found!");
            }

            database[ks_def.Name].Definitions = ks_def;
            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// string system_update_column_family(CfDef cf_def)
        /// </summary>
        public string system_update_column_family(CfDef cf_def)
        {
            if (!cf_def.__isset.keyspace)
            {
                throw new Exception("Keyspace not set!");
            }

            if (!cf_def.__isset.name)
            {
                throw new Exception("CF name not set!");
            }

            if (!database.ContainsKey(cf_def.Keyspace))
            {
                throw new Exception("Keyspace not found");
            }

            if (!database[cf_def.Keyspace].ContainsKey(cf_def.Name))
            {
                throw new Exception("Column Family not found");
            }

            database[cf_def.Keyspace][cf_def.Name].Definition = cf_def;
            return (++version).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Executes a CQL (Cassandra Query Language) statement and returns a CqlResult containing the results.
        /// Throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException.
        /// </summary>
        public CqlResult execute_cql_query(byte[] query, Compression compression)
        {
            throw new Exception("CQL not implemented");
        }

        /// <summary>
        /// Prepare a CQL (Cassandra Query Language) statement by compiling and returning
        /// - the type of CQL statement
        /// - an id token of the compiled CQL stored on the server side.
        /// - a count of the discovered bound markers in the statement
        /// </summary>
        public CqlPreparedResult prepare_cql_query(byte[] query, Compression compression)
        {
            throw new Exception("CQL not implemented");
        }

        /// <summary>
        /// Executes a prepared CQL (Cassandra Query Language) statement by passing an id token and a list of variables to bind 
        /// and returns a CqlResult containing the results.
        /// </summary>
        public CqlResult execute_prepared_cql_query(int itemId, List<byte[]> values)
        {
            throw new Exception("CQL not implemented");
        }

        public void set_cql_version(string version)
        {
            throw new NotImplementedException();
        }
    }
}
