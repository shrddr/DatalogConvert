### sql2dat

This tool fetches data from FactoryTalk SQL-based datalog and creates File Set datalog files with the same content.

How to use: first make sure you can connect to the SQL Server. Then run:

```
sql2dat PCname\SQLinstance master user password [TablePrefix]
```

Without `TablePrefix` the input tables are going to be `FloatTable` and `TagTable` (`StringTable` is not used).

The output is a bunch of daily DAT files like this:

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

If the trend still doesn't see the data generated this way, look into the DLG file created by FactoryTalk. It contains start/stop timestamps and limits access to files outside that range. I usually just delete it.

### dat2sql

You guessed it.

```
dat2sql PCname\SQLinstance master user password [TablePrefix]
```

The input is all files in current directory matched with `* (Float).DAT`

The `(Tagname)` files are assumed to be in the same directory.

Existing SQL tables will be dropped and recreated.

### dat2fth

Creates a CSV data file to import the file-based datalog into FactoryTalk Historian.

```
dat2fth PointPrefix [InputPattern]
```

`PointPrefix` refers to your Historian point names. If you create these using auto-discovery they are all going to start with `ViewAppName:HmiServerName:`. The auto-discovery also replaces `/` with `.` in tag names, so does this script.

`InputPattern` is the path to input files with filename wildcard. You can use the wildcard to filter the input by day or by month: `path/to/datalog/2019 12 * (Float).DAT`. If no `InputPattern` is specified, it's assumed to be `./* (Float).DAT`

Actual import is described in "PI Data Archive 2017 R2 System Management Guide" p.114 but the TLDR is:

1. Create all the Historian points manually
2. Force an archive shift in SMT
3. Run script

```
piconfig.exe < out.csv
```

The import is quite slow, for large datalogs you should probably try PI connectors instead. Also the CSV is almost twice the size of input DAT files so the storage might be an issue too.
