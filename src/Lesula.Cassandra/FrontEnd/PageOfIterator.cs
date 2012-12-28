// -----------------------------------------------------------------------
// <copyright file="PageOfIterator.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;

    using Apache.Cassandra;

    /*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

//package org.scale7.cassandra.pelops;

//import org.apache.cassandra.thrift.ConsistencyLevel;

//import java.util.Iterator;
//import java.util.List;
//import java.util.NoSuchElementException;

public abstract class PageOfIterator<T> : IIterator<T> {
    protected Selector selector;
    protected string columnFamily;
    protected byte[] rowKey;
    protected bool reversed;
    protected int batchSize;
    protected ConsistencyLevel cLevel;
    protected byte[]  startBeyondName;

    private IEnumerator<T> currentBatchIterator;

    public PageOfIterator(Selector selector, string columnFamily, byte[] rowKey,
                          byte[] startBeyondName, bool reversed, int batchSize,
                          ConsistencyLevel cLevel) {
        this.batchSize = batchSize;
        this.rowKey = rowKey;
        this.cLevel = cLevel;
        this.reversed = reversed;
        this.columnFamily = columnFamily;
        this.startBeyondName = startBeyondName;
        this.selector = selector;
    }

    private void fetchNextBatchInternal() {
        List<T> batch = fetchNextBatch();
        currentBatchIterator =  batch.GetEnumerator();
        startBeyondName = nextStartBeyondName(batch);
    }

    protected abstract List<T> fetchNextBatch();

    protected abstract byte[] nextStartBeyondName(List<T> batch);

    /**
     * Returns true if the iteration has more super columns. (In other words, returns true if next would return a super column rather than throwing an exception.)
     */
    //@Override
    public bool HasNext {
        get
        {
            if (currentBatchIterator == null)
                fetchNextBatchInternal();

            CheckNext();
            return nextAvailable;
        }
    }

    private bool fetchedNext = false;
    private bool nextAvailable = false;
    private T next;

    /**
     * Returns the next super column in the iteration.
     * @return the next super column
     * @throws java.util.NoSuchElementException iteration has no more super columns.
     */
    //@Override
    public T Next() {
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

    /**
     * Not supported.
     */
    //@Override
    public void Remove() {
        throw new InvalidOperationException();
    }

    public void Dispose()
    {
        currentBatchIterator.Dispose();
    }
}
}
