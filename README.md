# Cloud SQL import tool

`cloudsql-import` is a program resilient to restarts that replays a
mysql dump to a MySQL server. The resilience is gained by saving the
current state after each query. The final goal is for the program to
also speed up the replay by issuing some queries in parallel.

## How to run the tool

```
cloudsql-import --dump=dump.sql --dsn='USER:ROOT@tcp(X.X.X.X:3306)/YYYY'
```

Where `YYYY` is a (optional) database name.

## Licensing

- See [LICENSE][1]

[1]: LICENSE
