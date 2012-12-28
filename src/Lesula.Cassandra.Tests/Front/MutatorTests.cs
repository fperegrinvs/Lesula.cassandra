namespace Lesula.Cassandra.Tests.Front
{
    using System.Threading;

    using Apache.Cassandra;

    using Lesula.Cassandra.Client.Fake;
    using Lesula.Cassandra.FrontEnd;
    using Lesula.Cassandra.FrontEnd.Enumerators;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class MutatorTests
    {
        [TestInitialize]
        public void TestInit()
        {
            Monitor.Enter(TestHelper.CassandraLock);
            FakeCassandra.Init();
            var connection = TestHelper.GetCluster();
            var manager = new KeyspaceManager(connection);
            manager.AddKeyspace("Test", 1);

            var famManager = new ColumnFamilyManager(connection, "Test");
            famManager.TryAddColumnFamily("Sample", ColumnTypeEnum.Standard, ComparatorTypeEnum.UTF8Type);
            famManager.TryAddColumnFamily("Counter", ColumnTypeEnum.CounterStandard, ComparatorTypeEnum.UTF8Type);
        }

        [TestCleanup]
        public void Cleanup()
        {
            Monitor.Exit(TestHelper.CassandraLock);
        }

        [TestMethod]
        public void CreateCounterColumn()
        {
            var mutator = TestHelper.CreateMutator();
            mutator.WriteCounterColumn("Counter", "FooBar", "MyCounter", 10L);
            mutator.Execute(ConsistencyLevel.ONE);

            var selector = TestHelper.CreateSelector();
            var counter = selector.GetCounterColumnFromRow("Counter", "FooBar".ToBytes(), "MyCounter".ToBytes(), ConsistencyLevel.ONE);

            Assert.AreEqual(counter.Value, 10L);
        }

        [TestMethod]
        public void UpdateCounterColumnSingleMutation()
        {
            var mutator = TestHelper.CreateMutator();
            mutator.WriteCounterColumn("Counter", "FooBar", "MyCounter", 10L);
            mutator.WriteCounterColumn("Counter", "FooBar", "MyCounter", 10L);
            mutator.Execute(ConsistencyLevel.ONE);

            var selector = TestHelper.CreateSelector();
            var counter = selector.GetCounterColumnFromRow("Counter", "FooBar".ToBytes(), "MyCounter".ToBytes(), ConsistencyLevel.ONE);

            Assert.AreEqual(counter.Value, 20L);
        }

        [TestMethod]
        public void UpdateCounterColumnMultiMutation()
        {
            var mutator = TestHelper.CreateMutator();
            mutator.WriteCounterColumn("Counter", "FooBar", "MyCounter", 10L);
            mutator.Execute(ConsistencyLevel.ONE);

            mutator.WriteCounterColumn("Counter", "FooBar", "MyCounter", 10L);
            mutator.Execute(ConsistencyLevel.ONE);

            var selector = TestHelper.CreateSelector();
            var counter = selector.GetCounterColumnFromRow("Counter", "FooBar".ToBytes(), "MyCounter".ToBytes(), ConsistencyLevel.ONE);

            Assert.AreEqual(counter.Value, 20L);
        }

        [TestMethod]
        public void UpdateColumnSingleMutator()
        {
            var mutator = TestHelper.CreateMutator();
            var column = mutator.NewColumn("Foo", 3);
            mutator.WriteColumn("Sample", "FooBar", column);

            var column2 = mutator.NewColumn("Foo", 8);
            mutator.WriteColumn("Sample", "FooBar", column2);

            mutator.Execute(ConsistencyLevel.ONE);

            var selector = TestHelper.CreateSelector();
            var col = selector.GetColumnFromRow("Sample", "FooBar", "Foo", ConsistencyLevel.ONE);

            Assert.AreEqual(8, col.Value.ToInt32());
        }

        /// <summary>
        /// An update with old timestamp should be ignored
        /// </summary>
        [TestMethod]
        public void UpdateColumnLateUpdate()
        {
            var mutator = TestHelper.CreateMutator();
            var column = mutator.NewColumn("Foo", 3);
            mutator.WriteColumn("Sample", "FooBar", column);

            var column2 = mutator.NewColumn("Foo", 8);
            column2.Timestamp = column.Timestamp - 20;
            mutator.WriteColumn("Sample", "FooBar", column2);

            mutator.Execute(ConsistencyLevel.ONE);

            var selector = TestHelper.CreateSelector();
            var col = selector.GetColumnFromRow("Sample", "FooBar", "Foo", ConsistencyLevel.ONE);

            Assert.AreEqual(3, col.Value.ToInt32());
        }
    }
}
