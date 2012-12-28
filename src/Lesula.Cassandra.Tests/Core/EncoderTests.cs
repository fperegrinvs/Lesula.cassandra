namespace Lesula.Cassandra.Tests.Core
{
    using System;

    using Lesula.Cassandra.FrontEnd;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class EncoderTest
    {
        [TestMethod]
        public void BigEndianLongEncoderHelperTest()
        {
            long value = 8;
            byte[] javaByteLong = value.ToBytesBigEndian();
            long newValue = javaByteLong.ToInt64();
            Assert.AreEqual(value, newValue);
        }

        [TestMethod]
        public void UTF8EncoderHelperTest()
        {
            string value = "test value";
            byte[] bytes = value.ToBytes();
            string value2 = bytes.ToUtf8String();
            Assert.AreEqual(value, value2);
        }

        [TestMethod]
        public void GuidEncoderHelperTest()
        {
            Guid value = Guid.NewGuid();
            byte[] bytes = value.ToByteArray();
            Guid value2 = bytes.ToGuid();
            Assert.AreEqual(value, value2);
        }
    }
}
