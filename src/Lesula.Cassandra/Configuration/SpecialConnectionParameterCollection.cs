namespace Lesula.Cassandra.Configuration
{
    using System;
    using System.Configuration;

    /// <summary>
    /// Configuration Collection containing special connection parameters
    /// </summary>
    public class SpecialConnectionParameterCollection : ConfigurationElementCollection
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
        /// Creates and returns a new SpecialConnectionParameterElement element
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// </summary>
        /// <returns>a SpecialConnectionParameterElement object</returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new SpecialConnectionParameterElement();
        }

        /// <summary>
        /// Get the endpoint of a SpecialConnectionParameterElement
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="element">a SpecialConnectionParameterElement</param>
        /// <returns>the endpoint of the element</returns>
        protected override Object GetElementKey(ConfigurationElement element)
        {
            return ((SpecialConnectionParameterElement)element).Key;
        }

        /// <summary>
        /// returns the SpecialConnectionParameterElement in that position
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="index">position on the array</param>
        /// <returns>a SpecialConnectionParameterElement</returns>
        public SpecialConnectionParameterElement this[int index]
        {
            get
            {
                return (SpecialConnectionParameterElement)BaseGet(index);
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
        /// returns the SpecialConnectionParameterElement in that position
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="Name">name of the element</param>
        /// <returns>a SpecialConnectionParameterElement</returns>
        new public SpecialConnectionParameterElement this[string Name]
        {
            get
            {
                return (SpecialConnectionParameterElement)BaseGet(Name);
            }
        }
        /// <summary>
        /// Given a SpecialConnectionParameterElement, it returns its position on the array
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="element">a SpecialConnectionParameterElement contained on the array</param>
        /// <returns>the position of the element</returns>
        public int IndexOf(SpecialConnectionParameterElement element)
        {
            return BaseIndexOf(element);
        }

        /// <summary>
        /// Add SpecialConnectionParameterElement on the inner Array
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="element">element to be added</param>
        public void Add(SpecialConnectionParameterElement element)
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
        /// Remove the specified SpecialConnectionParameterElement from the inner Array
        /// </summary>
        /// <seealso cref="SpecialConnectionParameterElement"/>
        /// <param name="element">element to be removed</param>
        public void Remove(SpecialConnectionParameterElement element)
        {
            if (BaseIndexOf(element) >= 0)
                BaseRemove(element.Key);
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