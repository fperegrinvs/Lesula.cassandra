using System;

/// <summary>
/// The disposable extensions.
/// </summary>
/// <remarks>
/// Taken from https://github.com/pchalamet/cassandra-sharp
/// </remarks>
internal static class DisposableExtensions
{
    public static void SafeDispose(this IDisposable @this)
    {
        if (null != @this)
        {
            try
            {
                @this.Dispose();
            }
            // ReSharper disable EmptyGeneralCatchClause
            catch
            // ReSharper restore EmptyGeneralCatchClause
            {
            }
        }
    }

    public static void SafeDispose(this object @this)
    {
        var disposable = @this as IDisposable;
        disposable.SafeDispose();
    }
}