using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lesula.Cassandra.Tests
{
    using Lesula.Cassandra.Cluster;
    using Lesula.Cassandra.FrontEnd;

    public static class TestHelper
    {
        /// <summary>
        /// Default cluster
        /// </summary>
        /// <returns>
        /// The Lesula.Cassandra.Cluster.ICluster.
        /// </returns>
        internal static ICluster GetCluster()
        {
            const string DefaultCluster = "Lesula";
            return AquilesHelper.RetrieveCluster(DefaultCluster);
        }

        /// <summary>
        /// Construtor para facilitar a criação de um mutator.
        /// </summary>
        /// <param name="ttl">
        /// The ttl.
        /// </param>
        /// <returns>
        /// Mutator pronto para ser utilizado.
        /// </returns>
        public static Mutator CreateMutator(int ttl = Mutator.NoTtl)
        {
            return new Mutator(GetCluster(), "Test", ttl);
        }

        /// <summary>
        /// Construtor para facilitar a criação de um Selector.
        /// </summary>
        /// <returns>
        /// Selector pronto para ser utilizado.
        /// </returns>
        public static Selector CreateSelector()
        {
            return new Selector(GetCluster(), "Test");
        }
    }
}
