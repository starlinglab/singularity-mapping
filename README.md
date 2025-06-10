# singularity-mapping

```
$ export DATABASE_CONNECTION_STRING="mysql://singularity:your_password_here@tcp(127.0.0.1:3306)/singularity?parseTime=true"

$ go run ./main.go path/to/cars
Reading file: baga6ea4seaqnp2far3mvwgdwx3kowigj4omypvzw2kk6zjyqm4gnfplhaxqt4iy.car
Reading file: baga6ea4seaqenk7m5iwhkzvcygqww5x7vcbfqnnjalcvnx46etolybg7wtmsaay.car
Reading file: baga6ea4seaqczuepe4hbwb5sejy6nb33ufkoafwx75qq22s2jhtys7xmnscm4oa.car

$ sudo mariadb -u root -p
<snip>
MariaDB [(none)]> use singularity;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
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

If you re-run the command new rows will simply be appended, so you should drop the table first.
