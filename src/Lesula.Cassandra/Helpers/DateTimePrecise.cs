// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DateTimePrecise.cs" company="Lesula MapReduce Framework - http://github.com/lstern/lesula">
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//   
//    http://www.apache.org/licenses/LICENSE-2.0
//   
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// </copyright>
// <summary>
//   DateTimePrecise provides a way to get a DateTime that exhibits the
//   relative precision of
//   System.Diagnostics.Stopwatch, and the absolute accuracy of DateTime.Now.
// </summary>
// This code was taken from
// http://www.codeproject.com/KB/cs/DateTimePrecise.aspx?display=Print
// --------------------------------------------------------------------------------------------------------------------

namespace Lesula.Cassandra.Helpers
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// DateTimePrecise provides a way to get a DateTime that exhibits the
    /// relative precision of
    /// System.Diagnostics.Stopwatch, and the absolute accuracy of DateTime.Now.
    /// </summary>
    public class DateTimePrecise
    {
        /// Creates a new instance of DateTimePrecise.
        /// A large value of synchronizePeriodSeconds may cause arithmetic overthrow
        /// exceptions to be thrown. A small value may cause the time to be unstable.
        /// A good value is 10.
        /// synchronizePeriodSeconds = The number of seconds after which the
        /// DateTimePrecise will synchronize itself with the system clock.
        public DateTimePrecise(long synchronizePeriodSeconds)
        {
            this.Stopwatch = Stopwatch.StartNew();
            this.Stopwatch.Start();

            DateTime t = DateTime.UtcNow;
            this._immutable = new DateTimePreciseSafeImmutable(t, t, this.Stopwatch.ElapsedTicks, Stopwatch.Frequency);

            this._synchronizePeriodStopwatchTicks = synchronizePeriodSeconds *
                Stopwatch.Frequency;
        }

        /// Returns the current date and time, just like DateTime.UtcNow.
        public DateTime UtcNow
        {
            get
            {
                long s = this.Stopwatch.ElapsedTicks;
                DateTimePreciseSafeImmutable immutable = this._immutable;

                if (s < immutable._s_observed + this._synchronizePeriodStopwatchTicks)
                {
                    return immutable._t_base.AddTicks(((
                        s - immutable._s_observed) * _clockTickFrequency) / immutable._stopWatchFrequency);
                }

                DateTime t = DateTime.UtcNow;

                DateTime tBaseNew = immutable._t_base.AddTicks(((s - immutable._s_observed) * _clockTickFrequency) / immutable._stopWatchFrequency);

                this._immutable = new DateTimePreciseSafeImmutable(
                    t,
                    tBaseNew,
                    s,
                    ((s - immutable._s_observed) * _clockTickFrequency * 2)
                    /
                    (t.Ticks - immutable._t_observed.Ticks + t.Ticks + t.Ticks - tBaseNew.Ticks
                     - immutable._t_observed.Ticks));

                return tBaseNew;
            }
        }

        /// Returns the current date and time, just like DateTime.Now.
        public DateTime Now
        {
            get
            {
                return this.UtcNow.ToLocalTime();
            }
        }

        /// The internal System.Diagnostics.Stopwatch used by this instance.
        public Stopwatch Stopwatch;

        private long _synchronizePeriodStopwatchTicks;
        private const long _clockTickFrequency = 10000000;
        private DateTimePreciseSafeImmutable _immutable;
    }

    internal sealed class DateTimePreciseSafeImmutable
    {
        internal DateTimePreciseSafeImmutable(DateTime t_observed, DateTime t_base,
             long s_observed, long stopWatchFrequency)
        {
            this._t_observed = t_observed;
            this._t_base = t_base;
            this._s_observed = s_observed;
            this._stopWatchFrequency = stopWatchFrequency;
        }
        internal readonly DateTime _t_observed;
        internal readonly DateTime _t_base;
        internal readonly long _s_observed;
        internal readonly long _stopWatchFrequency;
    }
}
