namespace Lesula.Cassandra
{
    using System;

    /// <summary>
    /// Extensões do DateTime para facilitar o cálculo de chaves baseadas em periodos de tempo.
    /// </summary>
    public static class DateTimeExtensions
    {
        /// <summary>
        /// Data inicial para TotalDays/Hours/Months/Weeks
        /// </summary>
        private static DateTime startDate = new DateTime(2011, 1, 2);

        /// <summary>
        /// Data usada internamente para debug de agendamento
        /// </summary>
        public static DateTime? DebugDate { get; set; }

        /// <summary>
        /// Converte uma chave TotalHours/Days/Weeks/Months para DateTime
        /// </summary>
        /// <param name="key">A chave totaldays/etc</param>
        /// <returns>Datetime resultante</returns>
        public static DateTime ParseKeyString(string key)
        {
            var total = long.Parse(key.Substring(1));

            switch (key[0])
            {
                case 'H':
                    return startDate.AddHours(total);
                case 'D':
                    return startDate.AddDays(total);
                case 'W':
                    return startDate.AddDays(total * 7);
                case 'M':
                    return startDate.AddMonths((int)total);
            }

            throw new ArgumentException(string.Format("Chave inválida: {0}", key));
        }

        /// <summary>
        /// Retorna data corrente
        /// </summary>
        /// <returns>
        /// Data corrente
        /// </returns>
        public static DateTime GetCurrentDateTime()
        {
            return DebugDate ?? DateTime.Now;
        }

        /// <summary>
        /// Retorna data corrente
        /// </summary>
        /// <returns>
        /// Data corrente
        /// </returns>
        public static DateTime GetCurrentDateTimeUtc()
        {
            return DebugDate.HasValue ? DebugDate.Value.ToUniversalTime() : DateTime.UtcNow;
        }

        /// <summary>
        /// Calcula número de meses passados desde 01/2011
        /// </summary>
        /// <param name="dateTime">
        /// The date time.
        /// </param>
        /// <returns>
        /// Retorna números de meses passados desde 01/2011
        /// </returns>
        public static int TotalMonths(this DateTime dateTime)
        {
            return ((dateTime.Year - 2011) * 12) + dateTime.Month;
        }

        /// <summary>
        /// Calcula número de horas passadas desde 02/01/2011
        /// </summary>
        /// <param name="dateTime">
        /// The date time.
        /// </param>
        /// <returns>
        /// Retorna números de horas passadas desde 02/01/2011
        /// </returns>
        public static int TotalHours(this DateTime dateTime)
        {
            return (int)Math.Floor((dateTime - new DateTime(2011, 1, 2)).TotalHours);
        }

        /// <summary>
        /// Calcula número de semanas passadas desde 02/01/2011
        /// </summary>
        /// <param name="dateTime">
        /// The date time.
        /// </param>
        /// <returns>
        /// Retorna números de semanas passadas desde 02/01/2011
        /// </returns>
        public static int TotalWeeks(this DateTime dateTime)
        {
            return (int)Math.Floor((dateTime - new DateTime(2011, 1, 2)).TotalDays / 7);
        }

        /// <summary>
        /// Calcula número de dias passados desde 02/01/2011
        /// </summary>
        /// <param name="dateTime">
        /// The date time.
        /// </param>
        /// <returns>
        /// Retorna números de dias passados desde 02/01/2011
        /// </returns>
        public static int TotalDays(this DateTime dateTime)
        {
            return (int)Math.Floor((dateTime - new DateTime(2011, 1, 2)).TotalDays);
        }
    }
}
