namespace Lesula.Cassandra.Tests.Front
{
    using System;
    using System.Collections.Generic;

    using Apache.Cassandra;

    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.Helpers;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class DataDrivenTest
    {
        private const string CLUSTERNAME = "Lesula";
        private const string KEYSPACENAME = "exampleKeyspace";
        private const string COLUMNFAMILYNAME = "exampleCF";

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return this.testContextInstance;
            }
            set
            {
                this.testContextInstance = value;
            }
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            KsDef keyspaceDefinition = new KsDef();
            keyspaceDefinition.Name = KEYSPACENAME;
            keyspaceDefinition.Replication_factor = 1;
            keyspaceDefinition.Strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            keyspaceDefinition.Cf_defs = new List<CfDef>();
            var options = new Dictionary<string, string>();
            options["replication_factor"] = "1";
            keyspaceDefinition.Strategy_options = options;
            CfDef columnFamilyDefinition = new CfDef();
            columnFamilyDefinition.Keyspace = keyspaceDefinition.Name;
            columnFamilyDefinition.Comparator_type = "BytesType";
            columnFamilyDefinition.Key_cache_size = 20000;
            columnFamilyDefinition.Row_cache_save_period_in_seconds = 0;
            columnFamilyDefinition.Key_cache_save_period_in_seconds = 3600;
            columnFamilyDefinition.Name = COLUMNFAMILYNAME;

            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);

            var describeKeyspaces = new ExecutionBlock<List<KsDef>>(client => client.describe_keyspaces());

            List<KsDef> keyspaceDefinitions = cluster.Execute(describeKeyspaces);
            bool keyspaceExists = false;
            bool columnFamilyExists = false;
            if (keyspaceDefinitions != null)
            {
                foreach (KsDef ksDef in keyspaceDefinitions)
                {
                    if (ksDef.Name.CompareTo(keyspaceDefinition.Name) == 0)
                    {
                        keyspaceExists = true;
                        List<CfDef> columnFamilyDefinitions = ksDef.Cf_defs;
                        if (columnFamilyDefinitions != null)
                        {
                            foreach (CfDef cfDef in columnFamilyDefinitions)
                            {
                                if (cfDef.Name.CompareTo(columnFamilyDefinition.Name) == 0)
                                {
                                    columnFamilyExists = true;
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }
            }
            if (!keyspaceExists)
            {
                var createKeyspace = new ExecutionBlock<string>(client => client.system_add_keyspace(keyspaceDefinition));
                cluster.Execute(createKeyspace);
            }

            if (!columnFamilyExists)
            {
                var createColumnFamily = new ExecutionBlock<string>(client => client.system_add_column_family(columnFamilyDefinition));
                cluster.Execute(createColumnFamily, keyspaceDefinition.Name);
            }
        }

        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //    ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);

        //    ExecutionBlock drop_keyspace = new ExecutionBlock(delegate(Cassandra.Client client)
        //    {
        //        return client.system_drop_keyspace(KEYSPACENAME);
        //    });
        //    cluster.Execute(drop_keyspace);
        //}

        //
        // Use TestInitialize to run code before running each test 
        [TestInitialize()]
        public void MyTestInitialize()
        {
            this.PopulateDataWithInserts();
            this.PopulateDataWithBatchMutate();
        }

        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        private void PopulateDataWithInserts()
        {
            for (long i = 0; i < 100; i++)
            {
                byte[] key = i.ToBytesBigEndian();
                ColumnParent columnParent = new ColumnParent();
                Column column = new Column()
                {
                    Name = "Data".ToBytes(),
                    Timestamp = UnixHelper.UnixTimestamp,
                    Value = key
                };

                columnParent.Column_family = COLUMNFAMILYNAME;

                var cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
                cluster.Execute(
                    new ExecutionBlock<string>(
                        delegate(Cassandra.Iface client)
                            {
                                client.insert(key, columnParent, column, ConsistencyLevel.ONE);
                                return null;
                            }),
                    KEYSPACENAME);
            }
        }

        private void PopulateDataWithBatchMutate()
        {
            Dictionary<byte[], Dictionary<string, List<Mutation>>> mutation_map = new Dictionary<byte[], Dictionary<string, List<Mutation>>>();
            for (long i = 0; i < 100; i++)
            {
                byte[] key = i.ToBytesBigEndian();
                Dictionary<string, List<Mutation>> cfMutation = new Dictionary<string, List<Mutation>>();
                List<Mutation> mutationList = new List<Mutation>();
                for (long j = 0; j < 100; j++)
                {
                    string columnName = String.Format("Data-{0:0000000000}", j);
                    Mutation mutation = new Mutation()
                    {
                        Column_or_supercolumn = new ColumnOrSuperColumn()
                        {
                            Column = new Column()
                            {
                                Name = columnName.ToBytes(),
                                Timestamp = UnixHelper.UnixTimestamp,
                                Value = j.ToBytesBigEndian(),
                            },
                        },
                    };
                    mutationList.Add(mutation);
                }
                cfMutation.Add(COLUMNFAMILYNAME, mutationList);
                mutation_map.Add(key, cfMutation);
            }

            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            cluster.Execute(new ExecutionBlock<string>(delegate(Cassandra.Iface client)
            {
                client.batch_mutate(mutation_map, ConsistencyLevel.ONE);
                return null;
            }), KEYSPACENAME);
        }

        [TestMethod]
        public void testBatchMutateInsert()
        {
            byte[] key = 1L.ToBytesBigEndian();
            ColumnParent columnParent = new ColumnParent()
            {
                Column_family = COLUMNFAMILYNAME,
            };

            SlicePredicate predicate = new SlicePredicate()
            {
                Slice_range = new SliceRange()
                {
                    Count = 1000,
                    Reversed = false,
                    Start = new byte[0],
                    Finish = new byte[0],
                },
            };

            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            object rtnValue = cluster.Execute(delegate(Cassandra.Iface client)
                {
                    return client.get_count(key, columnParent, predicate, ConsistencyLevel.ONE);
                }, KEYSPACENAME);

            Int32 intValue = (Int32)rtnValue;
            Assert.AreEqual(101, intValue);
        }

        [TestMethod]
        public void TestBatchMutateOneColumn()
        {
            byte[] key = 2L.ToBytesBigEndian();
            var columnPath = new ColumnPath()
            {
                Column = "Data-0000000000".ToBytes(),
                Column_family = COLUMNFAMILYNAME,
            };

            var cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            object rtnValue = cluster.Execute(client => client.get(key, columnPath, ConsistencyLevel.ONE), KEYSPACENAME);

            Assert.IsNotNull(rtnValue);
        }

        //[TestMethod]
        public void SimplestTestWithKeyRange()
        {
            ColumnParent columnParent = new ColumnParent()
            {
                Column_family = COLUMNFAMILYNAME
            };

            SlicePredicate predicate = new SlicePredicate()
            {
                Slice_range = new SliceRange()
                {
                    Reversed = false,
                    Count = int.MaxValue,
                    Start = new byte[0]
                },
            };

            KeyRange keyRange = new KeyRange()
            {
                Count = 10,
                Start_key = 0.ToBytesBigEndian(),
                End_key = 50.ToBytesBigEndian()
            };

            List<KeySlice> output = null;
            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            cluster.Execute(new ExecutionBlock<string>(delegate(Cassandra.Iface client)
            {
                output = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
                return null;
            }), KEYSPACENAME);

            Assert.IsNotNull(output);
        }

        [TestMethod]
        public void TestWithKeyRangeGettingAllKeys()
        {
            ColumnParent columnParent = new ColumnParent()
            {
                Column_family = COLUMNFAMILYNAME
            };

            SlicePredicate predicate = new SlicePredicate()
            {
                Slice_range = new SliceRange()
                {
                    Reversed = false,
                    Count = int.MaxValue,
                    Start = new byte[0],
                    Finish = new byte[0]
                },
            };

            KeyRange keyRange = new KeyRange()
            {
                Count = 10,
                Start_token = "1",
                End_token = "0",
            };

            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            List<KeySlice> output = cluster.Execute<List<KeySlice>>(delegate(Cassandra.Iface client)
                {
                    output = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
                    return null;
                }, KEYSPACENAME);

            //Assert.IsNotNull(output);
        }

        [TestMethod]
        public void SimpleTestInsertCheckDeleteCheck()
        {
            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);

            byte[] key = Guid.NewGuid().ToString().ToBytes();
            byte[] columnName = Guid.NewGuid().ToString().ToBytes();
            byte[] columnValue = Guid.NewGuid().ToString().ToBytes();
            ColumnParent columnParent = new ColumnParent()
            {
                Column_family = COLUMNFAMILYNAME,
            };
            Column columnData = new Column()
            {
                Name = columnName,
                Timestamp = UnixHelper.UnixTimestamp,
                Value = columnValue,
            };

            // inserting data
            cluster.Execute(new ExecutionBlock<string>(delegate(Cassandra.Iface client)
            {
                client.insert(key, columnParent, columnData, ConsistencyLevel.ONE);
                return null;
            }), KEYSPACENAME);

            ColumnPath columnPath = new ColumnPath()
            {
                Column = columnName,
                Column_family = COLUMNFAMILYNAME,
            };
            ColumnOrSuperColumn columnOrSuperColumn = null;

            // getting inserted data
            cluster.Execute(delegate(Cassandra.Iface client)
                {
                    columnOrSuperColumn = client.get(key, columnPath, ConsistencyLevel.ONE);
                    return columnOrSuperColumn;
                }, KEYSPACENAME);

            Assert.IsNotNull(columnOrSuperColumn);
            Assert.IsNotNull(columnOrSuperColumn.Column);
            //Assert.AreEqual(columnOrSuperColumn.Column.Name, columnName);
            //Assert.AreEqual(columnOrSuperColumn.Column.Value, columnValue);

            // deleting data
            cluster.Execute(new ExecutionBlock<string>(delegate(Cassandra.Iface client)
            {
                client.remove(key, columnPath, UnixHelper.UnixTimestamp, ConsistencyLevel.ONE);
                return null;
            }), KEYSPACENAME);

            // verifying getting deleted data (there shouldn't be a response
            try
            {
                cluster.Execute(delegate(Cassandra.Iface client)
                    {
                        columnOrSuperColumn = client.get(key, columnPath, ConsistencyLevel.ONE);
                        return columnOrSuperColumn;
                    }, KEYSPACENAME);
            }
            catch (NotFoundException e)
            {
                columnOrSuperColumn = null;
            }

            Assert.IsNull(columnOrSuperColumn);
        }
    }
}
