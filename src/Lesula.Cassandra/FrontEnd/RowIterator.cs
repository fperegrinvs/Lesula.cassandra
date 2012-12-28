// -----------------------------------------------------------------------
// <copyright file="RowIterator.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;

    using Apache.Cassandra;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    /// <typeparam name="T">
    /// </typeparam>
    public abstract class RowIterator<T> : IIterator<KeyValuePair<byte[], List<T>>>
    {
        protected Selector selector;
        protected string columnFamily;
        protected byte[] startBeyondKey;
        protected int batchSize;
        protected SlicePredicate colPredicate;
        protected ConsistencyLevel cLevel;

        private bool isFirstBatch = true;
        private bool isMoreToFetch = false;
        private IEnumerator<KeyValuePair<byte[], List<T>>> currentBatchIterator;

        protected RowIterator(Selector selector, string columnFamily, byte[] startBeyondKey, int batchSize, SlicePredicate colPredicate, ConsistencyLevel cLevel)
        {
            this.selector = selector;
            this.columnFamily = columnFamily;
            this.startBeyondKey = startBeyondKey;
            this.batchSize = batchSize;
            this.colPredicate = colPredicate;
            this.cLevel = cLevel;
        }

        private void fetchNextBatchInternal()
        {
            var batch = this.FetchNextBatch();
            isMoreToFetch = batch.Count == this.batchSize;

            if (isFirstBatch)
            {
                isFirstBatch = false;
            }
            else
            {
                // we need to remove the first entry in the batch to avoid feeding through the iterator twice...
                batch.RemoveAt(0);
            }

            currentBatchIterator = batch.GetEnumerator();

        }
        private bool fetchedNext = false;
        private bool nextAvailable = false;
        private KeyValuePair<byte[], List<T>> next;

        void CheckNext()
        {
            if (!fetchedNext)
            {
                nextAvailable = currentBatchIterator.MoveNext();
                if (nextAvailable)
                {
                    next = currentBatchIterator.Current;
                }
                fetchedNext = true;
            }
        }

        protected abstract List<KeyValuePair<byte[], List<T>>> FetchNextBatch();

        public bool HasNext
        {
            get
            {
                if (currentBatchIterator == null)
                    fetchNextBatchInternal();

                CheckNext();
                return nextAvailable;
            }
        }

        public KeyValuePair<byte[], List<T>> Next()
        {
            if (currentBatchIterator == null)
                fetchNextBatchInternal();

            CheckNext();
            if (!nextAvailable)
            {
                throw new InvalidOperationException();
            }
            fetchedNext = false; // We've consumed this now
            return next;
        }

        public void Remove()
        {
            throw new InvalidOperationException();
        }

        public void Dispose()
        {
            currentBatchIterator.Dispose();
        }
    }
}
