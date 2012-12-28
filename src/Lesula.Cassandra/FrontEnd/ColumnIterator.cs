// -----------------------------------------------------------------------
// <copyright file="ColumnIterator.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System.Collections.Generic;
    using System.Linq;

    using Apache.Cassandra;


/**
 * Encapsulates the logic required to iterate over columns.  See
 * {@link Selector#iterateColumnsFromRow(string, org.scale7.cassandra.pelops.Bytes, org.scale7.cassandra.pelops.Bytes, boolean, int, org.apache.cassandra.thrift.ConsistencyLevel)}
 * for more detail.
 */
public class ColumnIterator : PageOfIterator<Column> {
    public ColumnIterator(Selector selector, string columnFamily, byte[] rowKey,
                               byte[] startBeyondName, bool reversed, int batchSize,
                               ConsistencyLevel cLevel) : base(selector, columnFamily, rowKey, startBeyondName, reversed, batchSize, cLevel) {
    }

    protected override List<Column> fetchNextBatch()
    {
        return this.selector.GetPageOfColumnsFromRow(
                this.columnFamily, this.rowKey, this.startBeyondName, this.reversed, this.batchSize, this.cLevel
        );
    }

    protected override byte[] nextStartBeyondName(List<Column> batch)
    {
        return (batch.Count == 0) ? new byte[0] : batch.Last().Name;
    }
}
}
