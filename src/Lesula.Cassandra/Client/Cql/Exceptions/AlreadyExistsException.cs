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

namespace Lesula.Cassandra.Client.Cql.Exceptions
{
    using System;

    using Lesula.Cassandra.Client.Cql.Enumerators;

    [Serializable]
    public class AlreadyExistsException : CassandraException
    {
        public AlreadyExistsException(string message, string keyspace, string table)
                : base(ErrorCode.AlreadyExists, message)
        {
            this.Keyspace = keyspace;
            this.Table = table;
        }

        public string Keyspace { get; private set; }

        public string Table { get; private set; }
    }
}