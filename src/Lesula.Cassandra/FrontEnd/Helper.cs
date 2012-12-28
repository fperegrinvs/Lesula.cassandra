// -----------------------------------------------------------------------
// <copyright file="Helper.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace Lesula.Cassandra.FrontEnd
{
    using System;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public static class Helper
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public static long ToTimestamp(this DateTimeOffset dt)
        {
            // this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
            return Convert.ToInt64((dt - UnixStart).Ticks / 10);
        }

        /// <summary>
        /// Converte um timetamp para um DateTime
        /// </summary>
        /// <param name="timestamp">
        /// The timestamp.
        /// </param>
        /// <returns>
        /// Datetime correspondente ao timestamp.
        /// </returns>
        public static DateTime FromTimestamp(this long timestamp)
        {
            return UnixStart.AddTicks(timestamp * 10).DateTime;
        }
    }
}
