// --------------------------------------------------------------------------------------------------------------------
// <copyright file="StructureTests.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
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
//   Defines the Structure tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Tests.Front
{
    using System;

    using Lesula.Cassandra.Client.Fake;
    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.FrontEnd;
    using Lesula.Cassandra.FrontEnd.Enumerators;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StructureTests
    {
        [TestInitialize]
        public void TestInit()
        {
            FakeCassandra.Init();
        }

        [TestMethod]
        public void CreateNewKeyspace()
        {
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            manager.AddKeyspace("Test", 1);

            var keyspace = manager.DescribeKeyspace("Test");
            Assert.AreEqual(keyspace.Replication_factor, 1);
            Assert.AreEqual(keyspace.Name, "Test");
        }

        [TestMethod]
        public void CreateExistingKeyspaceSucess()
        {
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            var status = manager.TryAddKeyspace("Test", 1);
            Assert.IsTrue(status);

            status = manager.TryAddKeyspace("Test", 1);
            Assert.IsFalse(status);

            var keyspace = manager.DescribeKeyspace("Test");
            Assert.AreEqual(keyspace.Replication_factor, 1);
            Assert.AreEqual(keyspace.Name, "Test");
        }

        [TestMethod]
        public void CreateNewColumnFamily()
        {
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            manager.AddKeyspace("Test", 1);

            var famManager = new ColumnFamilyManager(connection, "Test");
            var status = famManager.TryAddColumnFamily("Machine", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);
            Assert.IsTrue(status);

            var keyspace = manager.DescribeKeyspace("Test");
            Assert.AreEqual(1, keyspace.Cf_defs.Count);
            Assert.IsTrue(keyspace.Cf_defs.Exists(c => c.Name == "Machine"));
        }

        [TestMethod]
        public void CreateExistingColumnFamilySucess()
        {
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            manager.AddKeyspace("Test", 1);

            var famManager = new ColumnFamilyManager(connection, "Test");
            var status = famManager.TryAddColumnFamily("Machine", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);
            Assert.IsTrue(status);

            status = famManager.TryAddColumnFamily("Machine", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);
            Assert.IsFalse(status);

            var keyspace = manager.DescribeKeyspace("Test");
            Assert.AreEqual(1, keyspace.Cf_defs.Count);
            Assert.IsTrue(keyspace.Cf_defs.Exists(c => c.Name == "Machine"));
        }

        [TestMethod]
        public void CreateExistingColumnFamilyError()
        {
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            manager.AddKeyspace("Test", 1);

            var famManager = new ColumnFamilyManager(connection, "Test");
            famManager.TryAddColumnFamily("Machine", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);

            try
            {
                famManager.AddColumnFamily("Machine", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);
                Assert.Fail();
            }
            catch (Exception)
            {
                var keyspace = manager.DescribeKeyspace("Test");
                Assert.AreEqual(1, keyspace.Cf_defs.Count);
                Assert.IsTrue(keyspace.Cf_defs.Exists(c => c.Name == "Machine"));
            }
        }
    }
}
