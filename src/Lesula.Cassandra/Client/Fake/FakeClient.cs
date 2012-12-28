namespace Lesula.Cassandra.Client.Fake
{
    using System;
    using System.Reflection;

    using Apache.Cassandra;

    /// <summary>
    /// Cliente fake para testes com o cassandra
    /// </summary>
    public class FakeClient : AbstractClient
    {
        /// <summary>
        /// The send keyspace.
        /// </summary>
        private bool sendKeyspace = false;

        /// <summary>
        /// The keyspace name.
        /// </summary>
        private string keyspaceName;

        /// <summary>
        /// Keyspace
        /// </summary>
        public override string KeyspaceName
        {
            get
            {
                return this.keyspaceName;
            }

            set
            {
                this.keyspaceName = value;
                this.sendKeyspace = !string.IsNullOrEmpty(this.keyspaceName);
            }
        }

        /// <summary>
        /// Cliente cassandra
        /// </summary>
        public Cassandra.Iface CassandraClient { get; set; }

        /// <summary>
        /// indica se conexão está aberta
        /// </summary>
        private bool isOpen = false;

        /// <summary>
        /// Abrir conexão com o cliente
        /// </summary>
        public override void Open()
        {
            this.isOpen = true;
        }

        /// <summary>
        /// fechar conexão com o cliente
        /// </summary>
        public override void Close()
        {
            this.isOpen = false;
        }

        /// <summary>
        /// indica se a conexão está ativa ou não
        /// </summary>
        /// <returns></returns>
        public override bool IsOpen()
        {
            return this.isOpen;
        }

        /// <summary>
        /// executa uma operação no cassandra
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="executionBlock"></param>
        /// <returns></returns>
        public override T Execute<T>(ExecutionBlock<T> executionBlock)
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            if (this.sendKeyspace)
            {
                try
                {
                    this.sendKeyspace = false;
                    this.CassandraClient.set_keyspace(this.keyspaceName);
                }
                catch (Exception ex)
                {
                    throw DefaultClient.BuildException(ex);
                }
            }

            try
            {
                return executionBlock(this.CassandraClient);
            }
            catch (TargetInvocationException ex)
            {
                Exception inner = ex.InnerException;
                throw DefaultClient.BuildException(inner);
            }
        }

        /// <summary>
        /// Nome do cluster
        /// </summary>
        /// <returns>nome do cluster</returns>
        public override string getClusterName()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            return this.CassandraClient.describe_cluster_name();
        }
    }
}
