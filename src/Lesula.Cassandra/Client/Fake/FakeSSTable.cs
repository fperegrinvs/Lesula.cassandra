namespace Lesula.Cassandra.Client.Fake
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Numerics;
    using System.Security.Cryptography;

    using Apache.Cassandra;

    using Lesula.Cassandra.FrontEnd;
    using Lesula.Cassandra.Helpers;

    /// <summary>
    /// Fake SSLTable
    /// </summary>
    /// <remarks>
    /// Inspired from a python github gist
    /// https://gist.github.com/376249
    /// </remarks>
    public class FakeSSTable
    {
        private Dictionary<byte[], FakeRow> data;

        private SortedSet<byte[]> keys;

        /// <summary>
        /// Maps tokens to the corresponding key
        /// </summary>
        private Dictionary<byte[], byte[]> tokenMap;

        /// <summary>
        /// List of existing tokens
        /// </summary>
        private SortedSet<byte[]> tokens;

        /// <summary>
        /// Column Family Definition
        /// </summary>
        public CfDef Definition { get; set; }

        public FakeSSTable(Dictionary<byte[], FakeRow> myData = null)
        {
            this.Init(myData);
        }

        public void Init(Dictionary<byte[], FakeRow> myData = null)
        {
            if (myData == null)
            {
                myData = new Dictionary<byte[], FakeRow>(new ByteArrayComparer());
            }

            this.data = myData;
            this.keys = new SortedSet<byte[]>(new ByteArrayComparer());
            this.tokens = new SortedSet<byte[]>(new ByteArrayComparer());
            this.tokenMap = new Dictionary<byte[], byte[]>(new ByteArrayComparer());

            foreach (var myRow in myData)
            {
                this.AddKey(myRow.Key);
            }
        }

        private void AddKey(byte[] key)
        {
            this.keys.Add(key);
            var token = this.HashKeyBytes(key);
            this.tokens.Add(token);
            this.tokenMap.Add(token, key);
        }

        public void SetItem(byte[] key, FakeRow value)
        {
            if (!this.data.ContainsKey(key))
            {
                this.AddKey(key);
            }

            this.data[key] = value;
        }

        public void DelItem(byte[] key)
        {
            this.data.Remove(key);
            this.keys.Remove(key);

            var token = this.HashKeyBytes(key);
            this.tokens.Remove(token);
            this.tokenMap.Remove(token);
        }

        private long HashKeyMurmur(byte[] key)
        {
            if (key == null || key.Length == 0)
            {
                return 0L;
            }

            var hash = MurmurHash.hash3_x64_128(key, 0, key.Length, 0);
            return Math.Abs(hash[0]);
        }

        private byte[] HashKeyBytes(byte[] key)
        {
            var hash = MD5.Create().ComputeHash(key);
            var i = new BigInteger(hash);
            return BigInteger.Abs(i).ToByteArray();
        }

        public FakeRow GetItem(byte[] key)
        {
            return this.data[key];
        }

        public bool Contains(byte[] key)
        {
            return this.data.ContainsKey(key);
        }

        public int Len()
        {
            return this.keys.Count;
        }

        public FakeRow this[byte[] key]
        {
            get
            {
                return this.data[key];
            }

            set
            {
                this.SetItem(key, value);
            }
        }

        public void Clear()
        {
            this.data.Clear();
            this.keys.Clear();
            this.tokenMap.Clear();
            this.tokens.Clear();
        }

        public FakeSSTable Copy()
        {
            var newData = new Dictionary<byte[], FakeRow>(new ByteArrayComparer());
            foreach (var fakeRow in this.data)
            {
                newData[fakeRow.Key] = fakeRow.Value;
            }

            return new FakeSSTable(newData);
        }

        internal static List<byte[]> GetView(SortedSet<byte[]> values, byte[] start, byte[] finish, int? count = null, bool reversed = false, bool cyclic = false)
        {

            List<byte[]> view;
            if (start == null || start.Length == 0)
            {
                if (finish == null || finish.Length == 0)
                {
                    view = values.ToList();
                }
                else
                {
                    view = values.GetViewBetween(values.Min, finish).ToList();
                }
            }
            else
            {
                if (finish == null || finish.Length == 0)
                {
                    view = values.GetViewBetween(start, values.Max).ToList();
                }
                else
                {
                    if (cyclic && new ByteArrayComparer().Compare(start, finish) > 0)
                    {
                        view = values.GetViewBetween(finish, values.Max).ToList();
                        view.AddRange(values.GetViewBetween(values.Min, start));
                    }
                    else
                    {
                        view = values.GetViewBetween(start, finish).ToList();
                    }
                }
            }

            if (reversed)
            {
                view.Reverse();
            }

            if (count.HasValue)
            {
                if (count.Value > view.Count)
                {
                    count = view.Count;
                }

                view = view.GetRange(0, count.Value);
            }

            return view;
        }

        public List<byte[]> KeySlice(KeyRange keyRange)
        {
            List<FakeRow> results;

            if (keyRange.__isset.start_token || keyRange.__isset.end_token)
            {
                results = this.Slice(keyRange.Start_token.FromHexString(), keyRange.End_token.FromHexString(), keyRange.Count, false, false);
            }
            else
            {
                results = this.Slice(keyRange.Start_key, keyRange.End_key, keyRange.Count);
            }

            if (keyRange.__isset.row_filter)
            {
                results = keyRange.Row_filter.Aggregate(results, this.Filter);
            }

            return results.Select(r => r.Key).ToList();
        }

        private List<FakeRow> Filter(List<FakeRow> rows, IndexExpression filter)
        {

            switch (filter.Op)
            {
                case IndexOperator.LT:
                    return rows.Where(r => r.ContainsKey(filter.Column_name) && this.Compare(r[filter.Column_name], filter.Value) < 0).ToList();
                case IndexOperator.GTE:
                    return rows.Where(r => r.ContainsKey(filter.Column_name) && this.Compare(r[filter.Column_name], filter.Value) >= 0).ToList();
                case IndexOperator.GT:
                    return rows.Where(r => r.ContainsKey(filter.Column_name) && this.Compare(r[filter.Column_name], filter.Value) > 0).ToList();
                case IndexOperator.EQ:
                    return rows.Where(r => r.ContainsKey(filter.Column_name) && this.Compare(r[filter.Column_name], filter.Value) == 0).ToList();
                case IndexOperator.LTE:
                    return rows.Where(r => r.ContainsKey(filter.Column_name) && this.Compare(r[filter.Column_name], filter.Value) <= 0).ToList();
                default:
                    throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Compares ColumnOrSuperColumn with a byte array 
        /// </summary>
        /// <param name="cosc">column to be compared</param>
        /// <param name="value">reference value</param>
        /// <returns>negative if cosc value is lower than the reference value, 0 if they are equal and positive if the reference value is bigger</returns>
        private long Compare(ColumnOrSuperColumn cosc, byte[] value)
        {
            if (cosc.__isset.counter_column)
            {
                var longValue = value.ToInt64();
                return cosc.Counter_column.Value - longValue;
            }

            if (cosc.__isset.column)
            {
                if (cosc.Column.Value.Length > 8 || value.Length > 8)
                {
                    var strValue = cosc.Column.Value.ToUtf8String();
                    var refValue = value.ToUtf8String();
                    return string.CompareOrdinal(strValue, refValue);
                }

                var longValue = value.ToInt64();
                return cosc.Column.Value.ToInt64() - longValue;
            }

            throw new Exception("Column type not supported yet");
        }

        public List<byte[]> KeySlice(byte[] start, byte[] finish, int? count = null, bool reversed = false, bool convertKeyToToken = true)
        {
            if (convertKeyToToken)
            {
                return GetView(this.keys, start, finish, count, reversed);
            }

            var startToken = this.HashKeyBytes(start);
            var endToken = this.HashKeyBytes(finish);
            var view = GetView(this.tokens, startToken, endToken, count, reversed, true);
            return view.Select(token => this.tokenMap[token]).ToList();
        }

        public List<FakeRow> Slice(byte[] start, byte[] finish, int? count = null, bool reversed = false, bool convertKeyToToken = true)
        {
            var mykeys = this.KeySlice(start, finish, count, reversed, convertKeyToToken);
            return mykeys.Select(mykey => this.data[mykey]).ToList();
        }
    }
}