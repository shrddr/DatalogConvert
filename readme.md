This tool fetches data from FactoryTalk SQL-based datalog and creates File Set datalog files with the same content. This might help with your trend performance (if you don't know how to index your DB).

How to use: first make sure you can connect to the SQL Server. Then run:

```
DatalogConvert PCname\SQLinstance master user password
```

If you have non-default table names:

```
DatalogConvert PCname\SQLinstance master user password FloatTableName TagTableName StringTableName
```

This should generate a bunch of daily DAT files like this:

```
2018 10 27 0000 (Float).DAT
2018 10 27 0000 (String).DAT
2018 10 27 0000 (Tagname).DAT
2018 10 28 0000 (Float).DAT
2018 10 28 0000 (String).DAT
2018 10 28 0000 (Tagname).DAT
2018 10 30 0000 (Float).DAT
2018 10 30 0000 (String).DAT
2018 10 30 0000 (Tagname).DAT
```

Place the DAT files into your HMI Projects\AppName\DLGLOG\DatalogName. Make sure you switch the datalog storage format from ODBC to File Set so it starts reading the DAT files. Even that is not enough; in my tests, the trend was still reading **both** the SQL and file sets. If you don't want that, change the ODBC settings so the SQL server is no longer accessible (enter a non-existing username or remove the dsn file).

If the trend still doesn't see the data generated this way, look into the DLG file. It contains start/stop timestamps and limits access to files outside that range. My guess is you can simply delete it.
