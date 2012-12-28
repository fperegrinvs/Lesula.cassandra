namespace Lesula.Cassandra.Tests.Core
{
    using System;
    using System.Collections.Generic;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// This is a test class for GuidGeneratorTest and is intended
    /// to contain all GuidGeneratorTest Unit Tests
    /// </summary>
    [TestClass]
    public class GuidGeneratorTest
    {
        /// <summary>
        /// A test for GenerateTimeBasedGuid
        /// </summary>
        [TestMethod]
        public void GenerateTimeBasedGuidTest2()
        {
            const int cycles = 100;

            var generatedGuids = new HashSet<Guid>();
            for (int i = 0; i < cycles; i++)
            {
                var guid = Helpers.GuidGenerator.GenerateTimeBasedGuid();
                generatedGuids.Add(guid);
            }

            Assert.AreEqual(cycles, generatedGuids.Count);
        }
    }
}
