dbcopy
======

`dbcopy` is a database copy- and migration tool. It is used by the [Volkszaehler](http://volkszaehler.org) project.

Setup
-----

`dbcopy` is configured via a `dbcopy.json` config file. `dbcopy` will look for this file in the following places if not specified using the `--config` option:

  - current (working) directory
  - directory of `dbcopy` itself

The config file has the following structure:

```json
{
	"source": {
		// source database connection
		"driver": "pdo_mysql",
		"host": "localhost",
		"user": "travis",
		"password": "",
		"dbname": "volkszaehler"
	},
	"target": {
		// target database connection
		"driver": "pdo_sqlite",
		"path": "sqlite.db3",		// path is only used if driver = pdo_sqlite
		"host": "localhost",
		"user": "travis",
		"password": ""
		// "dbname": "backup"
	},
	"tables": [
		// table configuration (optional)
		// ------------------------------
		// table name
		// 		tables will be processed in the order they are mentioned:
		//		- foreign keys on target will be dropped
		//		- if a table is not listed here, it will not be touched
		// transfer mode
		//		skip:		table will not be copied
		//		copy:		entire table will be truncated on target and copied from source
		//		pk:			selective copy by primary key. only data not present on target
		// 						will be copied from source.
		{
			"name": "table_1",
			"mode": "copy"
		},
		...
	]
}
```

The `tables` section is optional. If not configured, `dbcopy` will auto-discover all tables in the source schema.

Command line syntax
-------------------

```
>./dbcopy
Database backup tool

Usage:
 [options] command [arguments]

Options:
 --help (-h) Display this help message.

Available commands:
 backup   Run backup
 clear    Clear target tables
 create   Create target schema
 drop     Drop target schema
 help     Displays help for a command
 influx   Copy data to InfluxDB
 list     Lists commands
```

### Create command

The `create` command will copy the database schema of the source database connection to the target database. If database drivers (e.g. MySQL and SQlite) differ, the schema definition will be translated for the target platform. Limits of the underlying **Doctrine/DBAL** libraries apply.

### Clear command

The `clear` command clears all tables in the target schema by TRUNCATING them. With the `--drop` option the tables are DROPPed altogether.

### Drop command

Opposed to `clear`, the `drop` command DROPs the entire target schema if the database platforms supports it. If e.g. SQlite is used, the SQlite database file is not deleted.

### Backup command

`backup` is the core routine of `dbcopy`. `backup` executes the following steps:

  1. Validate source connection is working and source schema exists, same for target connection and schema.
  2. Obtain list of tables- either from config file or by auto-discovering table of the source schema.
  3. Drop foreign keys on the identified tables in the *target* schema. This is necessary to allow copying of data without violating referential integrity.
  4. Copy data table by table according to configured copy mode.
  5. Re-apply foreign keys on the *target* schema (**Note:** currently not implemented for performance reasons.

### Influx command

The `influx` command can copy data from the Volkszaehler data table into an InfluxDB measurement. This can be helpful for using [Grafana](https://grafana.com) or [Chronograf](https://www.influxdata.com/time-series-platform/chronograf/) with Volkszaehler.

With this command, the table configuration is ignored and only the data table transferred, including additional entity attributes.

Limitations
-----------

`dbcopy` does not support triggers and stored procedures. No support is planned.

