Trite
=====

Trite is a client/server written in 100% Go that automates database restoration from XtraBackup files. Trite stands for <b>TR</b>ansport <b>I</b>nnodb <b>T</b>ables <b>E</b>fficiently and is a nod to the repetitive manual steps that must be done to use MySQL's [transportable tablespace] (http://dev.mysql.com/doc/en/tablespace-copying.html) feature. Copying binary files is much quicker than restoring with mysqldump when a table size on disk is very large. Trite allows partial database restoration not normally possible due to the design of InnoDB file per tablespaces and their relationship with the shared tablespace.

Typical use cases:  
* Restore a very large database that is all or mostly InnoDB tables quickly
* Clone a database to shrink the ibdata file size
* Perform a partial database restore
* Refresh a single database from different source databases


Dependencies
------------
[Go](http://golang.org/doc/install)  
[Git](http://git-scm.com/downloads) required for `go get`

Not required to compile the code but you won't be able to do much without:  
[Percona Server 5.1, 5.5, 5.6](http://www.percona.com/software/percona-server) or [Oracle MySQL 5.6](http://dev.mysql.com/downloads/mysql) or [MariaDB 5.5, 10](https://mariadb.com/resources/downloads)  
[Percona XtraBackup](http://www.percona.com/software/percona-xtrabackup)  

Installation
------------
```bash
$ go get github.com/joshuaprunier/trite
```

The compiled trite binary can be found at: $GOPATH/bin/trite

### Client Mode
Client mode restores database tables and code objects from a trite server. It must be run on the same server as the MySQL instance you are copying to and under a user that can write to the MySQL data directory.

### Dump Mode
Dump mode makes file copies of create statements for database tables and objects (procedures, functions, triggers, views). This is used in combination with an XtraBackup snapshot of a database when trite is run in server mode. A structure dump should be taken as close to the time a backup is done as possible to prevent backup/dump differences which may cause restoration errors. A subdirectory with a date/time stamp is created for dump files. Deletion or editing of objects in the dump directory can be done to customize what is restored in a database when a trite client is run. The MySQL server target can be local or remote in dump mode.

### Server Mode
Server mode starts an HTTP server that the trite client connects to download structure dump and xtrabackup files. Multiple trite servers can be run on the same server by specifying different ports and possibly different xtrabackup & structure dump locations. This is useful when restoring a master and slaves that have a subset of the master data.


Usage
-----
Trite has three modes of operation: client, dump or server  

```
  Usage of trite:

    CLIENT MODE
    ===========
    EXAMPLE: trite -client -user=myuser -pass=secret -socket=/var/lib/mysql/mysql.sock -triteServer=server1

    -client: Runs a trite client that downloads and applies database objects from a trite server
    -user: MySQL user name
    -pass: MySQL password (If omitted the user is prompted)
    -host: MySQL server hostname or ip
    -socket: MySQL socket file (socket is preferred over tcp if provided along with host)
    -port: MySQL server port (default 3306)
    -tls: Use TLS, also enables cleartext passwords (default false)
    -triteServer: Server name or ip of the trite server
    -tritePort: Port of trite server (default 12000)
    -triteMaxConnections: Maximum number of simultaneous database connections (default 20)
    -errorLog: File where details of an error is written (default trite.err in current working directory)
    -progressLimit: Limit size in GB that a file must be larger than for download progress to be displayed (default 5GB)

    DUMP MODE
    =========
    EXAMPLE: trite -dump -user=myuser -pass=secret -port=3306 -host=prod-db1 -dumpDir=/tmp

    -dump: Dumps create statements for tables & objects (prodecures, functions, triggers, views) from a local or remote MySQL database
    -user: MySQL user name
    -pass: MySQL password (If omitted the user is prompted)
    -host: MySQL server hostname or ip
    -socket: MySQL socket file (socket is preferred over tcp if provided along with host)
    -port: MySQL server port (default 3306)
    -tls: Use TLS, also enables cleartext passwords (default false)
    -dumpDir: Directory where dump files will be written (default current working directory)

    SERVER MODE
    ===========
    EXAMPLE: trite -server -dumpPath=/tmp/trite_dump20130824_173000 -backupPath=/tmp/xtrabackup_location

    -server: Runs a HTTP server allowing a trite client to download xtrabackup and database object dump files
    -dumpPath: Path to create statement dump files
    -backupPath: Path to xtraBackup files
    -tritePort: Port of trite server (default 12000)
```


Limitations & Caveats
------------------------------
* Trite's speed is largely dependent on network transfer speed from the server to the client and the i/o speed of the database destination. A small amount of CPU is consumed when restoring compressed InnoDB tables.
* innodb_file_per_table must be enabled on both the xtrabackup source database and the destination.
* The import process bypasses MySQL replication so care must be given when restoring a database master or slave.
* The destination database must be running Percona server 5.1, 5.5, 5.6 or Oracle MySQL 5.6 or MariaDB 5.5, 10.
* The --export & --apply-log options must be run on the database backup taken with Percona XtraBackup. Running trite in server mode will throw an error and exit if this has not been done.
* Currently only InnoDB & MyISAM storage engines are supported by trite. Additional engines should be easy to add provided they are supported by XtraBackup.
* The mysql, information_schema and performance_schema are ignored in dump mode.
* The import process is very verbose and will pollute the MySQL error log with information for every table imported. Unfortunately there is no way to prevent this.
* Import of compressed InnoDB tables is noted as "EXPERIMENTAL" but has worked just fine in my testing except for being rather slow.
* Care should be taken customizing a restore. Problems may occur removing a table but not a trigger on it or not restoring a table referenced by another tables foreign key.

To Do
-----
[See enhancements in issues] (https://github.com/joshuaprunier/trite/issues?labels=enhancement&page=1&state=open)

Thanks
------
I would like to extend my thanks to:
- [Netprospex](http://www.netprospex.com) for allowing me to open source this project and being an awesome place to work!
- The MySQL community for the great wealth of information out there to keep dba's and developers educated.
- Percona for great open source software that makes my job easier.
- All the contributors to Go making it such an effective and fun language to learn and code with.
- My wife Diane for being so supportive and understanding with my hands glued to a keyboard for the past few months.

License
-------
Trite is licensed under an MIT license. Details can be found in the [LICENSE](https://github.com/joshuaprunier/trite/raw/master/LICENSE) file.
