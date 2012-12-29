using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lesula.Cassandra.Tests.Cql
{
    using Lesula.Cassandra.Client.Cql.Enumerators;

    /// <summary>
    /// Summary description for CqlBinaryTests
    /// </summary>
    [TestClass]
    public class CqlBinaryTests
    {
        public CqlBinaryTests()
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
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
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
        public void Connect()
        {
            const string DefaultCluster = "CqlLesula";
            var client = AquilesHelper.RetrieveCluster(DefaultCluster);
            client.ExecuteNonQueryAsync("USE mytest", CqlConsistencyLevel.ONE);
        }
    }
}
