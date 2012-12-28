namespace Lesula.Cassandra.FrontEnd
{
    using System;
    using System.Collections.Generic;

    using Apache.Cassandra;

    using Lesula.Cassandra.Exceptions;

    public class Validation {

    public static byte[] safeGetRowKey(byte[] rowKey) {
    	if (rowKey == null || rowKey.Length == 0)
            throw new AquilesException("Row Key is null");
    	return rowKey;
    }

    public static List<byte[]> validateRowKeys(List<byte[]> rowKeys) {
    	foreach (byte[] b in rowKeys)
    		validateRowKey(b);
    	return rowKeys;
    }

    public static List<string> validateRowKeysUtf8(List<string> rowKeys) {
    	foreach (string s in rowKeys)
    		if (s == null)
                throw new AquilesException("Row key is null");
    	return rowKeys;
    }

    public static byte[] validateRowKey(byte[] rowKey)
    {
    	if (rowKey == null || rowKey.Length == 0)
            throw new AquilesException("Row Key is null");
    	return rowKey;
    }

    public static void validateColumn(Column column) {
    	if (!column.__isset.name)
            throw new AquilesException("Column name is null");
    	if (!column.__isset.value)
            throw new AquilesException("Column value is null");
	}

    public static void validateColumn(CounterColumn column) {
        if (!column.__isset.name)
            throw new AquilesException("Column name is null");
        if (!column.__isset.value)
            throw new AquilesException("Column value is null");
	}

    public static void validateColumns(List<Column> columns) {
    	foreach (Column c in columns) validateColumn(c);
	}

    public static void validateCounterColumns(List<CounterColumn> columns) {
    	foreach (CounterColumn c in columns) validateColumn(c);
	}

    public static void validateColumnNames(List<byte[]> names) {
        foreach (byte[] n in names) validateColumnName(n);
	}

    public static void validateColumnName(byte[] name)
    {
		if (name == null || name.Length == 0)
            throw new AquilesException("Column name is null");
	}
}
}
