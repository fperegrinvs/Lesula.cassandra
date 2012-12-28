// -----------------------------------------------------------------------
// <copyright file="ColumnRowIterator.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System.Collections.Generic;

    using Apache.Cassandra;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class ColumnRowIterator : RowIterator<Column>
    {
        public ColumnRowIterator(Selector selector, string columnFamily, byte[] startBeyondKey, int batchSize, SlicePredicate colPredicate, ConsistencyLevel cLevel)
            : base(selector, columnFamily, startBeyondKey, batchSize, colPredicate, cLevel)
        {
        }

        protected override List<KeyValuePair<byte[], List<Column>>> FetchNextBatch()
        {
            return this.selector.GetColumnsFromRows(
                    this.columnFamily, Selector.NewKeyRange(this.startBeyondKey, new byte[0], this.batchSize), this.colPredicate, this.cLevel
            );
        }
    }
}
