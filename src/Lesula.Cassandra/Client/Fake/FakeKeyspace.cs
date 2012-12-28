namespace Lesula.Cassandra.Client.Fake
{
    using System.Collections.Generic;

    using Apache.Cassandra;

    public class FakeKeyspace : Dictionary<string, FakeSSTable>
    {
        public KsDef Definitions { get; set; }
    }
}
