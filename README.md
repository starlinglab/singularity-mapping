# singularity-mapping

Singularity chops up files into "ranges" of 1G each. This tool looks at the output CAR files
to see which CAR files contain the file ranges, as matched by CID.

For more info on Singularity CARs, see [NOTES.md](./NOTES.md).

```
$ export DATABASE_CONNECTION_STRING="mysql://singularity:your_password_here@tcp(127.0.0.1:3306)/singularity?parseTime=true"

$ go run ./main.go path/to/cars
<some output>

$ sudo mariadb -u root -p singularity
<snip>
MariaDB [singularity]> SELECT * FROM file_range_car;
+---------------+--------+
| file_range_id | car_id |
+---------------+--------+
|             1 |      1 |
|             2 |      1 |
|             3 |      1 |
|             4 |      1 |
|             5 |      1 |
<snip>
```

Currently all data is added in a transaction, so if you cancel halfway through the database will
not be updated.

This command can be re-run and only new information will be added to the `file_range_car` table.
