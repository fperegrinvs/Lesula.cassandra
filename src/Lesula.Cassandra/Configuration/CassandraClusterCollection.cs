// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CassandraClusterCollection.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   Configuration Collection containing Cassandra Cluster information
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;

    /// <summary>
    /// Configuration Collection containing Cassandra Cluster information
    /// </summary>
    public class CassandraClusterCollection : ConfigurationElementCollection
    {
        /// <summary>
        /// get the CollectionType for this collection
        /// </summary>
        public override ConfigurationElementCollectionType CollectionType
        {
            get
            {
                return ConfigurationElementCollectionType.AddRemoveClearMapAlternate;
            }
        }

        /// <summary>
        /// Creates and returns a new CassandraClusterElement element
        /// <seealso cref="CassandraClusterElement"/>
        /// </summary>
        /// <returns>a CassandraClusterElement object</returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new CassandraClusterElement();
        }

        /// <summary>
        /// Get the endpoint of a CassandraClusterElement
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="element">a CassandraClusterElement</param>
        /// <returns>the endpoint of the element</returns>
        protected override Object GetElementKey(ConfigurationElement element)
        {
            return ((CassandraClusterElement)element).FriendlyName;
        }

        /// <summary>
        /// returns the CassandraClusterElement in that position
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="index">position on the array</param>
        /// <returns>a CassandraClusterElement</returns>
        public CassandraClusterElement this[int index]
        {
            get
            {
                return (CassandraClusterElement)BaseGet(index);
            }

            set
            {
                if (this.BaseGet(index) != null)
                {
                    this.BaseRemoveAt(index);
                }

                this.BaseAdd(index, value);
            }
        }

        /// <summary>
        /// returns the CassandraClusterElement in that position
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="Name">name of the element</param>
        /// <returns>a CassandraClusterElement</returns>
        new public CassandraClusterElement this[string Name]
        {
            get
            {
                return (CassandraClusterElement)BaseGet(Name);
            }
        }
        /// <summary>
        /// Given a CassandraClusterElement, it returns its position on the array
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="element">a CassandraClusterElement contained on the array</param>
        /// <returns>the position of the element</returns>
        public int IndexOf(CassandraClusterElement element)
        {
            return BaseIndexOf(element);
        }

        /// <summary>
        /// Add CassandraClusterElement on the inner Array
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="element">element to be added</param>
        public void Add(CassandraClusterElement element)
        {
            BaseAdd(element);
        }

        /// <summary>
        /// Inner BaseAdd method
        /// </summary>
        /// <param name="element"></param>
        protected override void BaseAdd(ConfigurationElement element)
        {
            BaseAdd(element, false);
        }

        /// <summary>
        /// Remove the specified CassandraClusterElement from the inner Array
        /// </summary>
        /// <seealso cref="CassandraClusterElement"/>
        /// <param name="element">element to be removed</param>
        public void Remove(CassandraClusterElement element)
        {
            if (BaseIndexOf(element) >= 0)
                BaseRemove(element.FriendlyName);
        }

        /// <summary>
        /// Remove the element at the specific position on the array
        /// </summary>
        /// <param name="index">index of the element</param>
        public void RemoveAt(int index)
        {
            BaseRemoveAt(index);
        }

        /// <summary>
        /// Remove the specific element with the given name from the inner array
        /// </summary>
        /// <param name="name">name of the element</param>
        public void Remove(string name)
        {
            BaseRemove(name);
        }

        /// <summary>
        /// Clear the inner Array
        /// </summary>
        public void Clear()
        {
            BaseClear();
        }
    }


}