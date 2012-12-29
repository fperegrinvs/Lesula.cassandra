namespace Lesula.Cassandra.Client.Cql
{
    using System;
    using System.IO;
    using System.Threading;

    using Lesula.Cassandra.Client.Cql.Enumerators;

    public class CqlClient : AbstractClient
    {
        /// <summary>
        /// The stream id
        /// </summary>
        private byte StreamId { get; set; }

        /// <summary>
        /// The input stream
        /// </summary>
        private Stream InputStream { get; set; }

        /// <summary>
        /// The output stream
        /// </summary>
        private Stream OutputStream { get; set; }

        /// <summary>
        /// Current Message Header
        /// </summary>
        public CqlMessageHeader Header { get; set; }

        public bool IsBusy { get; set; }

        /// <summary>
        /// Event fired when the client is available for new requests
        /// </summary>
        public event Action Available;

        /// <summary>
        /// Used to block and release threads manually.
        /// </summary>
        private readonly ManualResetEvent callerBlocker = new ManualResetEvent(true);

        /// <summary>
        /// Used to block and release threads manually.
        /// </summary>
        private readonly ManualResetEvent readerBlocker = new ManualResetEvent(true);

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlClient"/> class.
        /// </summary>
        /// <param name="id">
        /// The id.
        /// </param>
        /// <param name="inputStream">
        /// The input stream.
        /// </param>
        /// <param name="outputStream">
        /// The output stream.
        /// </param>
        public CqlClient(byte id, Stream inputStream, Stream outputStream)
        {
            this.IsBusy = false;
            this.StreamId = id;
            this.InputStream = inputStream;
            this.OutputStream = outputStream;
        }

        /// <summary>
        /// The read response.
        /// </summary>
        /// <param name="message">
        /// The message.
        /// </param>
        internal void ReadResponse(CqlMessageHeader message)
        {
            if (message.StreamId != this.StreamId)
            {
                throw new ArgumentException("wrong streamId");
            }

            if (message.Direction == MessageDirection.Request)
            {
                throw new ArgumentException("Received a request message instead of a Response one.");
            }

            this.Header = message;

            // Release the caller thread to process the results
            this.callerBlocker.Set();
            this.readerBlocker.WaitOne();
        }

        #region Overrides of AbstractClient

        public override string KeyspaceName
        {
            get
            {
                throw new NotImplementedException("This method is for thrift clients only.");
            }

            set
            {
                throw new NotImplementedException("This method is for thrift clients only.");
            }
        }

        private bool isOpen;

        public override void Open()
        {
            this.isOpen = true;
        }

        public override void Close()
        {
            this.isOpen = false;
        }

        public override bool IsOpen()
        {
            return this.isOpen;
        }

        public override T Execute<T>(ExecutionBlock<T> executionBlock)
        {
            throw new NotImplementedException("This method is for thrift clients only.");
        }

        public override T QueryAsync<T>(string cql, ICqlObjectBuilder<T> builder, CqlConsistencyLevel cl)
        {
            this.BeginRequest(cql, cl);

            try
            {
                var results = this.ProcessResponse(builder);
                this.EndRequest();
                return results;
            }
            catch (Exception)
            {
                this.EndRequest();
                throw;
            }
        }

        private T ProcessResponse<T>(ICqlObjectBuilder<T> buider)
        {
            switch (this.Header.Operation)
            {
                case CqlOperation.Result:
                    return FrameReader.ReadResult(this.Header, this.InputStream, buider);
                case CqlOperation.Ready:
                case CqlOperation.Authenticate:
                case CqlOperation.Supported:
                    FrameReader.ReadBody(this.Header, this.InputStream, true);
                    throw new ArgumentException("Invalid response");
                case CqlOperation.Error:
                    // will throw exception
                    FrameReader.ReadBody(this.Header, this.InputStream, true);
                    throw new Exception("Unknown exception");
                default:
                    FrameReader.ReadBody(this.Header, this.InputStream, true);
                    throw new ArgumentException("Invalid response");
            }
        }

        public override string ExecuteNonQueryAsync(string cql, CqlConsistencyLevel cl)
        {
            return this.QueryAsync<string>(cql, null, cl);
        }

        private void BeginRequest(string cql, CqlConsistencyLevel cl)
        {
            // set as busy.
            this.IsBusy = true;

            // hold the reader thread to avoid anyone reading the stream
            this.readerBlocker.Reset();

            // query cassandra
            var writer = new FrameWriter(this.OutputStream, this.StreamId);
            writer.SendQuery(cql, cl, CqlOperation.Query);

            // block this thread to wait for response (the response will unblock this thread)
            this.callerBlocker.Reset();

            // waits for respose
            this.callerBlocker.WaitOne();
        }

        private void EndRequest()
        {
            // client is free again, release the reader
            this.readerBlocker.Set();
            this.IsBusy = false;
        }

        public override string getClusterName()
        {
            throw new System.NotImplementedException();
        }

        #endregion
    }
}
