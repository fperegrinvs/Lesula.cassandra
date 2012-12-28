namespace Lesula.Cassandra.Client
{
    using System;
    using System.Globalization;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Connection.Pooling;
    using Lesula.Cassandra.Model;

    public abstract class AbstractClient : IClient
    {
        #region IClient Members

        public virtual IClientPool OwnerPool
        {
            get;
            set;
        }

        public abstract string KeyspaceName { get; set; }

        public virtual IEndpoint Endpoint
        {
            get;
            set;
        }

        public abstract void Open();

        public abstract void Close();

        public abstract bool IsOpen();

        public abstract T Execute<T>(ExecutionBlock<T> executionBlock);

        public abstract string getClusterName();

        #region IDisposable Members
        protected bool disposed = false;
        public void Dispose()
        {
            this.Dispose(true);
        }

        private void Dispose(bool forceDispose)
        {
            this.disposed = true;
            this.OwnerPool = null;
        }
        #endregion

        #endregion

        protected static string BuildExceptionMessage(Exception ex)
        {
            return BuildExceptionMessage(ex, null);
        }
        protected static string BuildExceptionMessage(Exception ex, string why)
        {
            if (string.IsNullOrEmpty(why))
            {
                return string.Format(CultureInfo.CurrentCulture, "Exception '{0}' during executing command. See inner exception for further details.", ex.Message);
            }
            else
            {
                return string.Format(CultureInfo.CurrentCulture, "Exception '{0}' during executing command: '{1}'. See inner exception for further details.", ex.Message, why);
            }
        }

        protected bool Invalid
        {
            get;
            set;
        }

        public override string ToString()
        {
            return string.Format("{0}-{1}", base.ToString(), this.GetHashCode());
        }
    }
}
