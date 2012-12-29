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
    using System.Collections.Generic;

    using Lesula.Cassandra.Client.Cql.Enumerators;

    public interface IFrameWriter
    {
        void WriteShort(short data);

        void WriteInt(int data);

        void WriteString(string data);

        void WriteShortByteArray(byte[] data);

        void WriteLongString(string data);

        void WriteStringMap(Dictionary<string, string> dic);

        void WriteStringList(string[] data);

        void WriteByteArray(byte[] data);

        void Send(CqlOperation msgOpcode);
    }
}