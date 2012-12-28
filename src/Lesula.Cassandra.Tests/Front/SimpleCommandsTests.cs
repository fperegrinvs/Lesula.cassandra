namespace Lesula.Cassandra.Tests.Front
{
    using System.Collections.Generic;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Client;
    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.Exceptions;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Summary description for CommandsTest
    /// </summary>
    [TestClass]
    public class SimpleCommandsTest
    {
        private const string CLUSTERNAME = "Lesula";

        public SimpleCommandsTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

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
            AquilesHelper.Initialize();
        }

        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod]
        public void SimpleGetClusterNameExecutionBlockOnCluster()
        {
            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            string clusterName2 = (string)cluster.Execute(this.getClusterName);
            Assert.IsNotNull(clusterName2);
        }

        [TestMethod]
        public void AccessingNonExistantKeyspace()
        {
            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            try
            {
                cluster.Execute(this.getClusterName, "lala");
            }
            catch (ExecutionBlockException)
            {
                // this is expected
            }
        }

        [TestMethod]
        public void AccessingNonExistantKeyspaceBorrowingClient()
        {
            bool isClientHealthy = true, noException = true;
            ICluster cluster = AquilesHelper.RetrieveCluster(CLUSTERNAME);
            IClient client = cluster.Borrow("lala");
            try
            {
                client.Execute(this.getClusterName);
            }
            catch (ExecutionBlockException ex)
            {
                noException = false;
                isClientHealthy = ex.IsClientHealthy;
            }
            finally
            {
                if (noException || isClientHealthy)
                {
                    cluster.Release(client);
                }
                else
                {
                    cluster.Invalidate(client);
                }
            }
        }

        [TestMethod]
        public void ABMOnKeyspaceAndColumnFamily()
        {
            KsDef keyspaceDefinition = new KsDef();
            keyspaceDefinition.Name = "exampleKeyspace";
            keyspaceDefinition.Replication_factor = 1;
            keyspaceDefinition.Strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
            var options = new Dictionary<string, string>();
            options["replication_factor"] = "1";
            keyspaceDefinition.Strategy_options = options;
            keyspaceDefinition.Cf_defs = new List<CfDef>();
            CfDef columnFamilyDefinition = new CfDef();
            columnFamilyDefinition.Keyspace = keyspaceDefinition.Name;
            columnFamilyDefinition.Comparator_type = "BytesType";
            columnFamilyDefinition.Key_cache_size = 20000;
            columnFamilyDefinition.Row_cache_save_period_in_seconds = 0;
            columnFamilyDefinition.Key_cache_save_period_in_seconds = 3600;
            columnFamilyDefinition.Name = "exampleCF";

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
            try
            {
                if (!keyspaceExists)
                {
                    ExecutionBlock<string> create_keyspace = delegate(Cassandra.Iface client)
                        {
                            return client.system_add_keyspace(keyspaceDefinition);
                        };
                    cluster.Execute(create_keyspace);
                }

                if (!columnFamilyExists)
                {
                    var create_columnFamily = new ExecutionBlock<string>(client => client.system_add_column_family(columnFamilyDefinition));
                    cluster.Execute(create_columnFamily, keyspaceDefinition.Name);
                }
            }
            finally
            {
                var drop_keyspace = new ExecutionBlock<string>(client => client.system_drop_keyspace(keyspaceDefinition.Name));
                cluster.Execute(drop_keyspace);
            }
        }

        public string getClusterName(Cassandra.Iface client)
        {
            return client.describe_cluster_name();
        }
    }
}
