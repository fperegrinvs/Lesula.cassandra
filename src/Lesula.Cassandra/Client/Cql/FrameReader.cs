﻿// cassandra-sharp - a .NET client for Apache Cassandra
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
    using Lesula.Cassandra.Client.Cql.Exceptions;
    using Lesula.Cassandra.Client.Cql.Extensions;

    internal class FrameReader : IFrameReader, IDisposable
    {
        private const byte VersionMask = 0x7F;

        internal static byte ProtocolVersion = 0x01;

        private readonly Stream ms;

        private FrameReader(CqlMessageHeader header, Stream stream, bool streaming)
        {
            this.MessageOpcode = header.Operation;

            if (streaming)
            {
                this.ms = new WindowedReadStream(stream, header.Size);
            }
            else
            {
                var buffer = new byte[header.Size];
                if (0 < header.Size)
                {
                    stream.Read(buffer, 0, header.Size);
                    this.ms = new MemoryStream(buffer);
                }
            }

            if (CqlOperation.Error == this.MessageOpcode)
            {
                using (this)
                {
                    this.ThrowError();
                }
            }
        }

        public void Dispose()
        {
            this.ms.SafeDispose();
        }

        public CqlOperation MessageOpcode { get; private set; }

        public byte ReadByte()
        {
            return (byte)this.ms.ReadByte();
        }

        public short ReadShort()
        {
            return this.ms.ReadShort();
        }

        public int ReadInt()
        {
            return this.ms.ReadInt();
        }

        public string ReadString()
        {
            return this.ms.ReadString();
        }

        public string[] ReadStringList()
        {
            return this.ms.ReadStringList();
        }

        public byte[] ReadBytes()
        {
            return this.ms.ReadBytes();
        }

        public byte[] ReadShortBytes()
        {
            return this.ms.ReadShortBytes();
        }

        public Dictionary<string, string[]> ReadStringMultimap()
        {
            return this.ms.ReadStringMultimap();
        }

        public static FrameReader ReadBody(CqlMessageHeader header, Stream stream, bool streaming)
        {
            return new FrameReader(header, stream, streaming);
        }

        public static T ReadResult<T>(CqlMessageHeader header, Stream stream, ICqlObjectBuilder<T> buider)
        {
            var responseType = (CqlResultKind)stream.ReadInt();
            switch (responseType)
            {
                case CqlResultKind.SchemaChange:
                case CqlResultKind.SetKeyspace:
                case CqlResultKind.Rows:
                case CqlResultKind.Prepared:
                case CqlResultKind.Void:
                    break;
            }

            var buffer = new byte[header.Size - 4];
            stream.Read(buffer, 0, buffer.Length);
            return default(T);
        }


        public static CqlMessageHeader ProcessHeader(byte[] headerBytes)
        {
            var header = new CqlMessageHeader();

            if (headerBytes.Length != 8)
            {
                throw new ArgumentException("Invalid Header");
            }

            var version = headerBytes[0];
            if (0 == (version & (byte)MessageDirection.Response))
            {
                throw new ArgumentException("Expecting response frame");
            }

            header.Direction = MessageDirection.Response;
            header.Version = (byte)(version & VersionMask);
            if (header.Version != ProtocolVersion)
            {
                throw new ArgumentException("Unknown protocol version");
            }

            header.Flags = (CqlHeaderFlags)headerBytes[1];
            header.StreamId = headerBytes[2];
            header.Operation = (CqlOperation)headerBytes[3];
            header.Size = BitConverter.ToInt32(headerBytes, 4).ReverseBytes();
            return header;
        }

        private void ThrowError()
        {
            ErrorCode code = (ErrorCode)this.ms.ReadInt();
            string msg = this.ms.ReadString();

            switch (code)
            {
                case ErrorCode.Unavailable:
                    {
                        CqlConsistencyLevel cl = (CqlConsistencyLevel)this.ReadShort();
                        int required = this.ReadInt();
                        int alive = this.ReadInt();
                        throw new UnavailableException(msg, cl, required, alive);
                    }

                case ErrorCode.WriteTimeout:
                    {
                        CqlConsistencyLevel cl = (CqlConsistencyLevel)this.ReadShort();
                        int received = this.ReadInt();
                        int blockFor = this.ReadInt();
                        string writeType = this.ReadString();
                        throw new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                    }

                case ErrorCode.ReadTimeout:
                    {
                        CqlConsistencyLevel cl = (CqlConsistencyLevel)this.ReadShort();
                        int received = this.ReadInt();
                        int blockFor = this.ReadInt();
                        bool dataPresent = 0 != this.ReadByte();
                        throw new ReadTimeOutException(msg, cl, received, blockFor, dataPresent);
                    }

                case ErrorCode.Syntax:
                    throw new SyntaxException(msg);

                case ErrorCode.Unauthorized:
                    throw new UnauthorizedException(msg);

                case ErrorCode.Invalid:
                    throw new InvalidException(msg);

                case ErrorCode.AlreadyExists:
                    {
                        string keyspace = this.ReadString();
                        string table = this.ReadString();
                        throw new AlreadyExistsException(msg, keyspace, table);
                    }

                case ErrorCode.Unprepared:
                    {
                        byte[] unknownId = this.ReadShortBytes();
                        throw new UnpreparedException(msg, unknownId);
                    }

                default:
                    throw new CassandraException(code, msg);
            }
        }
    }
}