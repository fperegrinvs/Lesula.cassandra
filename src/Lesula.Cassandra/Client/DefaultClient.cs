namespace Lesula.Cassandra.Client
{
    using System;
    using System.IO;
    using System.Reflection;

    using Apache.Cassandra;

    using Lesula.Cassandra;
    using Lesula.Cassandra.Exceptions;

    using Thrift;
    using Thrift.Transport;

    public class DefaultClient : AbstractClient
    {
        private bool sendKeyspace = false;

        private Cassandra.Client cassandraClient;
        public Cassandra.Client CassandraClient
        {
            get
            {
                return this.cassandraClient;
            }
            set
            {
                this.cassandraClient = value;
            }
        }

        private string keyspaceName;
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

        public override void Open()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            if (this.CassandraClient != null)
            {
                this.CassandraClient.InputProtocol.Transport.Open();
                if (!this.CassandraClient.InputProtocol.Transport.Equals(this.CassandraClient.OutputProtocol.Transport))
                {
                    this.CassandraClient.OutputProtocol.Transport.Open();
                }
            }
            else
            {
                throw new AquilesException("Cannot open a client when no cassandra client was associated with it.");
            }
        }

        public override void Close()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            this.CassandraClient.InputProtocol.Transport.Close();
            if (!this.CassandraClient.InputProtocol.Transport.Equals(this.CassandraClient.OutputProtocol.Transport))
            {
                this.CassandraClient.OutputProtocol.Transport.Close();
            }
        }

        public override bool IsOpen()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            if ((this.CassandraClient != null)
                && (this.CassandraClient.InputProtocol != null)
                && (this.CassandraClient.OutputProtocol != null)
                && (this.CassandraClient.InputProtocol.Transport != null)
                && (this.CassandraClient.OutputProtocol.Transport != null))
            {
                return this.CassandraClient.InputProtocol.Transport.IsOpen && this.CassandraClient.OutputProtocol.Transport.IsOpen;
            }

            return false;
        }

        public override string getClusterName()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DefaultClient");
            }

            return this.CassandraClient.describe_cluster_name();
        }

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
                    this.cassandraClient.set_keyspace(this.keyspaceName);
                }
                catch (Exception ex)
                {
                    throw BuildException(ex);
                }
            }

            try
            {
                return executionBlock(this.CassandraClient);
            }
           catch (TargetInvocationException ex)
            {
                Exception inner = ex.InnerException;
                throw BuildException(inner);
            }
        }

        internal static ExecutionBlockException BuildException(Exception exception)
        {
            Type exceptionType = exception.GetType();

            if (exceptionType.Equals(typeof(NotFoundException)))
            {
                // WTF?? "not found" should be trapped.
                string error = BuildExceptionMessage(exception);

                // despite this command failed, the connection is still usable.
                return new ExecutionBlockException(error, exception) { IsClientHealthy = true, ShouldRetry = false };
            } 
            else if (exceptionType.Equals(typeof(InvalidRequestException)))
            {
                var exception2 = (InvalidRequestException)exception;
                string error = BuildExceptionMessage(exception, exception2.Why);

                // despite this command failed, the connection is still usable.
                return new ExecutionBlockException(error, exception) { IsClientHealthy = true, ShouldRetry = false };
            }
            else if (exceptionType.Equals(typeof(UnavailableException)))
            {
                // WTF?? Cassandra cannot ensure object replication state?
                string error = BuildExceptionMessage(exception);
                return new ExecutionBlockException(error, exception) { IsClientHealthy = true, ShouldRetry = true };
            }
            else if (exceptionType.Equals(typeof(TimedOutException)))
            {
                // WTF?? Cassandra timeout?
                string error = BuildExceptionMessage(exception);
                return new ExecutionBlockException(error, exception) { IsClientHealthy = true, ShouldRetry = true };
            }
            else if (exceptionType.Equals(typeof(TApplicationException)))
            {
                // client thrift version does not match server version?
                string error = BuildExceptionMessage(exception);
                return new ExecutionBlockException(error, exception) { IsClientHealthy = false, ShouldRetry = false };
            }
            else if (exceptionType.Equals(typeof(AuthenticationException)))
            {
                var exception2 = (AuthenticationException)exception;

                // invalid credentials
                string error = BuildExceptionMessage(exception, exception2.Why);

                // despite this command failed, the connection is still usable.
                return new ExecutionBlockException(error, exception2) { IsClientHealthy = true, ShouldRetry = false };
            }
            else if (exceptionType.Equals(typeof(AuthorizationException)))
            {
                var exception2 = (AuthorizationException)exception;

                // user does not have access to keyspace
                string error = BuildExceptionMessage(exception2, exception2.Why);
                
                // despite this command failed, the connection is still usable.
                return new ExecutionBlockException(error, exception2) { IsClientHealthy = true, ShouldRetry = false };
            }
            else if (exceptionType.Equals(typeof(TTransportException)))
            {
                // user does not have access to keyspace
                string error = string.Format("{0} This might happen when the transport configured on the client does not match the server configuration.", BuildExceptionMessage(exception));

                // this connection is screwed
                return new ExecutionBlockException(error, exception) { IsClientHealthy = false, ShouldRetry = true };
            }
            else if (exceptionType.Equals(typeof(IOException)))
            {
                // i got the client, it was opened but the node crashed or something
                AquilesHelper.Reset();

                string error = BuildExceptionMessage(exception);
                return new ExecutionBlockException(error, exception) { IsClientHealthy = false, ShouldRetry = true };
            }
            else
            {
                return new ExecutionBlockException("Unhandled exception.", exception);
            }
        }
    }
}
