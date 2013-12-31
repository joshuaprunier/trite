Trite
=====

Trite is a client/server written in 100% Go that provides customizable transportation of binary InnoDB table files from XtraBackups. Trite stands for <b>TR</b>ansport <b>I</b>nnodb <b>T</b>ables <b>E</b>fficiently and is also a nod to the repetitive manual steps that must be done to copy .ibd files to remote MySQL databases. Trite automates that manual process. Copying binary files is much quicker than traditional mysqldump restores when a tables size becomes very large. It also allows partial database restores not normally possible due to the design of InnoDB file per tablespaces and their relationship with the shared tablespace.  

Trite is a good fit if time is a factor and you need to do the following:  
* Copy very large InnoDB tables
* Recover just a subset of database tables
* Refresh a single database instance from multiple source databases
* Get an additional benefit from those stagnant database backups ;-)

The [dependencies](https://github.com/joshuaprunier/trite/edit/master/README.md#dependencies) and [limitations](https://github.com/joshuaprunier/trite/edit/master/README.md#limitations-caveats) are rather extensive so be sure to read them before deciding if trite will work in your environment.


Dependencies
------------
[Go 1.0](http://golang.org/doc/install) or greater  
[Go MySQL Driver](http://github.com/go-sql-driver/mysql) or equivalent conforming to Go's [database/sql](http://golang.org/pkg/database/sql/) package  
[Go net/html sub-repo](http://godoc.org/code.google.com/p/go.net/html) package is used to parse html pages served by trite in server mode   

Not required to compile the code but you won't be able to do much without:  
[Percona Server 5.1/5.5](http://percona.com)  
[Percona XtraBackup](http://percona.com)  

Installation
------------
Install [git](http://git-scm.com/downloads) and [mercurial](http://mercurial.selenic.com/downloads/) if necessary. (Needed for `go get` of the following code)

Get the latest copy of trite
```bash
$ go get github.com/joshuaprunier/trite
```

Get the latest copy of go-sql-driver
```bash
$ go get github.com/go-sql-driver/mysql
```

Get the latest copy of the net/html sub-repo
```bash
$ go get code.google.com/p/go.net/html
```

Build trite. The compiled binary can then be copied to another location including different servers of the same architecture.
```bash
$ go build trite.go
```

Usage
-----
Trite is run in one of three modes: client, dump or server  

```
  Usage of trite:

    CLIENT MODE
    ===========
    EXAMPLE: trite -client -user=myuser -password=secret -socket=/var/lib/mysql/mysql.sock -server_host=server1 -workers=3

    -client: Runs locally on the database you wish to copy files to and connects to an trite server
    -user: MySQL user name
    -password: MySQL password (If omitted the user is prompted)
    -host: MySQL server hostname or ip
    -socket: MySQL socket file (socket is preferred over tcp if provided along with host)
    -port: MySQL server port (default 3306)
    -datadir: MySQL data directory (default is mysql users homedir, mainly used for multi instances)
    -server_host: Server name or ip hosting the backup and dump files
    -server_port: Port of trite server (default 12000)
    -workers: Number of copy threads (default 1)

    DUMP MODE
    =========
    EXAMPLE: trite -dump -user=myuser -password=secret -port=3306 -host=prod-db1 -dump_dir=/tmp

    -dump: Dumps create statements for tables & objects (prodecures, functions, triggers, views) from a local or remote MySQL database
    -user: MySQL user name
    -password: MySQL password (If omitted the user is prompted)
    -host: MySQL server hostname or ip
    -socket: MySQL socket file (socket is preferred over tcp if provided along with host)
    -port: MySQL server port (default 3306)
    -dump_dir: Directory to where dump files will be written (default current working directory)

    SERVER MODE
    ===========
    EXAMPLE: trite -server -dump_path=/tmp/trite_dump20130824_173000 -backup_path=/tmp/xtrabackup_location

    -server: Runs an HTTP server serving the backup and database object dump files
    -dump_path: Path to dump files
    -backup_path: Path to XtraBackup files
    -server_port: Port of trite server (default 12000)
```

### Client Mode
Client mode copies database tables and code objects from a trite server. It must be run on the same server as the MySQL instance you are copying to and under a user that can write to the MySQL data directory.

	trite -client -user=myuser -password=secret -socket=/var/lib/mysql/mysql.sock -server_url=http://trite-server -workers=5
* <b>-user</b> MySQL user name.
* <b>-password</b> Password for the MySQL user. (If omitted the user is prompted)
* <b>-host</b> Host name or ip address of the MySQL server. (If socket and host are blank localhost is assumed)
* <b>-socket</b> Socket file of the MySQL server.
* <b>-port</b> Port of MySQL server. (default 3306)
* <b>-server_host</b> Server name or ip of the trite server hosting the backup and dump files.
* <b>-server_port</b> Port of the trite server. (default 12000)
* <b>-workers</b> Number of worker threads that will download and import tables concurrently. The number of worker threads will depend primarily on the i/o capacity of the MySQL server hardware and the speed of your network connection. (default 1)

### Dump Mode
Dump mode makes file copies of create statements for database tables and objects (procedures, functions, triggers, views). This is used in combination with an XtraBackup snapshot of a database when trite is run in server mode. A structure dump should be taken as close to the time a backup is done as possible to prevent backup/dump differences which may cause restoration errors. A subdirectory with a date/time stamp is created for dump files. Deletion or editing of objects in the dump directory can be done to customize what is restored in a database when a trite client is run. The MySQL server target can be local or remote in dump mode.

	trite -dump -user=myuser -password=secret -port=3306 -host=prod-db1 -dump_dir=/tmp
* <b>-user</b> MySQL user name.
* <b>-password</b> Password for the MySQL user. (If omitted the user is prompted)
* <b>-host</b> Host name or ip address of the MySQL server. (If socket and host are blank localhost is assumed)
* <b>-socket</b> Socket file of the MySQL server.
* <b>-port</b> Port of MySQL server. (default 3306)
* <b>-dump_dir</b> Directory to dump create statements for database tables and objects. (default is current working directory)

### Server Mode
Server mode starts an HTTP server that the trite client connects to. It is run by supplying the -dump_path and -backup_path flags. Multiple trite servers can be run simultaneously with different -dump_path, -backup_path and -server_port configurations . This is useful when restoring a master and slave that has a subset of the master data.

	trite -server -dump_path=/tmp/trite_dump20130824_173000 -backup_path=/tmp/xtrabackup_location
* <b>-dump_path</b> Path to a database structure dump created by running trite in -dump mode. (default is current working directory)
* <b>-backup_path</b> Path to an XtraBackup where --export & --apply-log has been run. When server mode is started it will check the backups and ensure tables have properly been prepared for export, otherwise it exits with an error.
* <b>-server_port</b> Port the HTTP server will listen on. (default 12000)


Limitations & Caveats
------------------------------
* Trite's speed is largely dependent on network transfer speed from the server to the client and the i/o speed of the database destination. A small amount of CPU is consumed when restoring compressed InnoDB tables.
* innodb_file_per_table must be enabled on both the database backed up from and restored to.
* The import process bypasses MySQL replication so care must be given when restoring a database master or slave.
* The destination database must be running Percona server 5.1 or 5.5
* The --export & --apply-log options must be run on the database backup taken with Percona XtraBackup. This is checked when a trite server is started otherwise it will exit with an error.
* Running --export & --apply-log prevents the backup from being used with additional incremental backups. An LVM snapshot/restore should get around this.
* Currently only InnoDB & MyISAM engines are supported by trite. Additional engines should be easy to add provided they are supported by XtraBackup.
* The mysql, information_schema and performance_schema are ignored in dump mode.
* The import process is very verbose and will pollute the MySQL error log with information for every table imported.
* Import of compressed InnoDB tables is noted as "EXPERIMENTAL" but has worked just fine in my testing. Please let me know if you experience otherwise.
* Table transfer failure is currently detected, cleaned up and the code terminates. This does not happen under typical use but I plan to add retry code eventually.
* It is currently up to the human customizing the restore to handle table relationships with regards to foreign key relationships. Fk constraints are ignored during the copy process.

To Do
-----
* Compression for copying across slow networks.
* Retry of failed transfers.
* Replication support - auto change master to
* Test cases.
* Automatically adjust worker threads based on load.

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
