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
    using System.Collections.Generic;
    using System.IO;

    using Lesula.Cassandra.Client.Cql.Enumerators;
    using Lesula.Cassandra.Client.Cql.Extensions;

    internal class FrameWriter : IFrameWriter, IDisposable
    {
        private readonly Stream _stream;

        private readonly byte _streamId;

        private readonly MemoryStream _ms;

        internal FrameWriter(Stream stream, byte streamId)
        {
            this._stream = stream;
            this._streamId = streamId;
            this._ms = new MemoryStream();
        }

        public void Dispose()
        {
            this._stream.Flush();
            this._ms.SafeDispose();
        }

        public void Send(CqlOperation msgOpcode)
        {
            const byte version = (byte)(FrameType.Request | FrameType.ProtocolVersion);
            this._stream.WriteByte(version);

            const byte flags = (byte)FrameHeaderFlags.None;
            //if (compress)
            //{
            //    flags |= 0x01;
            //}
            this._stream.WriteByte(flags);

            // streamId
            this._stream.WriteByte(this._streamId);

            // opcode
            this._stream.WriteByte((byte)msgOpcode);

            // len of body
            int bodyLen = (int)this._ms.Length;
            this._stream.WriteInt(bodyLen);

            // body
            this._stream.Write(this._ms.GetBuffer(), 0, bodyLen);
            this._stream.Flush();
        }

        public void WriteShort(short data)
        {
            this._ms.WriteShort(data);
        }

        public void WriteInt(int data)
        {
            this._ms.WriteInt(data);
        }

        public void WriteString(string data)
        {
            this._ms.WriteString(data);
        }

        public void WriteShortByteArray(byte[] data)
        {
            this._ms.WriteShortByteArray(data);
        }

        public void WriteLongString(string data)
        {
            this._ms.WriteLongString(data);
        }

        public void WriteStringMap(Dictionary<string, string> dic)
        {
            this._ms.WriteStringMap(dic);
        }

        public void WriteStringList(string[] data)
        {
            this._ms.WriteStringList(data);
        }

        public void WriteByteArray(byte[] data)
        {
            this._ms.WriteByteArray(data);
        }
    }
}