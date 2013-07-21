// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Lesula.Cassandra.Client.Cql
{
    using System;
    using System.IO;

    internal class BuilderReadStream : Stream
    {
        private readonly WindowedReadStream baseStream;

        private int len;

        public BuilderReadStream(WindowedReadStream baseStream)
        {
            this.baseStream = baseStream;
            this.len = baseStream.len;
        }

        public byte[] FinishReadingWindow()
        {
            var buffer = new byte[this.len];
            this.Read(buffer, 0, buffer.Length);
            return buffer;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // skip unread data
                while (0 < this.len)
                {
                    --this.len;
                    this.baseStream.ReadByte();
                }
            }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count == 0)
            {
                return 0;
            }

            if (this.len < count)
            {
                count = this.len;
            }

            this.len -= count;

            int read = this.baseStream.Read(buffer, offset, count);
            if (count != read)
            {
                throw new IOException("Unexpected read count");
            }

            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}