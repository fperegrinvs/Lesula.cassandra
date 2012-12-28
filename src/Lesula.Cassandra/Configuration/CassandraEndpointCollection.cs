namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;

    /// <summary>
    /// Configuration Collection containing cassandraEndpoints
    /// </summary>
    public class CassandraEndpointCollection : ConfigurationElementCollection
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CassandraEndpointCollection()
        {
        }

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
        /// Creates and returns a new CassandraEndpointElement element
        /// <seealso cref="CassandraEndpointElement"/>
        /// </summary>
        /// <returns>a CassandraEndpointElement object</returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new CassandraEndpointElement();
        }

        /// <summary>
        /// Get the endpoint of a CassandraEndpointElement
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="element">a CassandraEndpointElement</param>
        /// <returns>the endpoint of the element</returns>
        protected override Object GetElementKey(ConfigurationElement element)
        {
            return ((CassandraEndpointElement)element).Address;
        }

        /// <summary>
        /// returns the CassandraEndpointElement in that position
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="index">position on the array</param>
        /// <returns>a CassandraEndpointElement</returns>
        public CassandraEndpointElement this[int index]
        {
            get
            {
                return (CassandraEndpointElement)BaseGet(index);
            }
            set
            {
                if (BaseGet(index) != null)
                {
                    BaseRemoveAt(index);
                }
                BaseAdd(index, value);
            }
        }

        /// <summary>
        /// returns the CassandraEndpointElement in that position
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="Name">name of the element</param>
        /// <returns>a CassandraEndpointElement</returns>
        new public CassandraEndpointElement this[string Name]
        {
            get
            {
                return (CassandraEndpointElement)BaseGet(Name);
            }
        }

        /// <summary>
        /// Given a CassandraEndpointElement, it returns its position on the array
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="element">a CassandraEndpointElement contained on the array</param>
        /// <returns>the position of the element</returns>
        public int IndexOf(CassandraEndpointElement element)
        {
            return BaseIndexOf(element);
        }

        /// <summary>
        /// Add CassandraEndpointElement on the inner Array
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="element">element to be added</param>
        public void Add(CassandraEndpointElement element)
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
        /// Remove the specified CassandraEndpointElement from the inner Array
        /// </summary>
        /// <seealso cref="CassandraEndpointElement"/>
        /// <param name="element">element to be removed</param>
        public void Remove(CassandraEndpointElement element)
        {
            if (BaseIndexOf(element) >= 0)
                BaseRemove(element.Address);
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