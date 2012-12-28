namespace Lesula.Cassandra
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Comparador para array de bytes.
    /// </summary>
    public class ByteArrayComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
    {
        /// <summary>
        /// Compara 2 byte[] e diz se são iguais
        /// </summary>
        /// <param name="left">
        /// The left.
        /// </param>
        /// <param name="right">
        /// The right.
        /// </param>
        /// <returns>
        /// true caso sejam iguais
        /// </returns>
        public bool Equals(byte[] left, byte[] right)
        {
            if (left == null || right == null)
            {
                return left == right;
            }

            return left.SequenceEqual(right);
        }

        /// <summary>
        /// Hashcode para byteArray
        /// </summary>
        /// <param name="key">
        /// The key.
        /// </param>
        /// <returns>
        /// Hash para o byteArray
        /// </returns>
        public int GetHashCode(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            return Encoding.UTF8.GetString(key).GetHashCode();  
        }

        #region Implementation of IComparer<in byte[]>

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <returns>
        /// A signed integer that indicates the relative values of <paramref name="x"/> and <paramref name="y"/>, as shown in the following table.Value Meaning Less than zero<paramref name="x"/> is less than <paramref name="y"/>.Zero<paramref name="x"/> equals <paramref name="y"/>.Greater than zero<paramref name="x"/> is greater than <paramref name="y"/>.
        /// </returns>
        /// <param name="x">The first object to compare.</param><param name="y">The second object to compare.</param>
        public int Compare(byte[] x, byte[] y)
        {
            var strX = Encoding.UTF8.GetString(x);
            var strY = Encoding.UTF8.GetString(y);

            return string.CompareOrdinal(strX, strY);
        }

        #endregion
    }
}
