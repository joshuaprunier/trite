package main

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"runtime/pprof"
	"strconv"
	"time"
)

// ShowUsage prints a help screen which details all three modes command line flags
func showUsage() {
	fmt.Println(`
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
  `)
}

// Main
func main() {
	start := time.Now()

	// Catch signals
	catchNotifications()

	// Get working directory
	wd, err := os.Getwd()
	checkErr(err)

	// Profiling flags
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")

	// MySQL flags
	flagDbUser := flag.String("user", "", "MySQL: User")
	flagDbPass := flag.String("password", "", "MySQL: Password")
	flagDbHost := flag.String("host", "", "MySQL: Host")
	flagDbPort := flag.String("port", "3306", "MySQL: Port")
	flagDbSock := flag.String("socket", "", "MySQL: Socket")

	// Client flags
	flagClient := flag.Bool("client", false, "Run in client mode")
	flagServerHost := flag.String("server_host", "", "CLIENT: Server URL")
	flagWorkers := flag.Uint("workers", 1, "Number of concurrent worker threads for downloading & table importing")

	// Dump flags
	flagDump := flag.Bool("dump", false, "Run in dump mode")
	flagDumpDir := flag.String("dump_dir", wd, "DUMP: Directory for output")

	// Server flags
	flagServer := flag.Bool("server", false, "Run in server mode")
	flagTablePath := flag.String("dump_path", "", "SERVER: Path to create table files")
	flagBackupPath := flag.String("backup_path", "", "SERVER: Path to database backup files")
	flagPort := flag.String("server_port", "12000", "CLIENT/SERVER: HTTP port number") // also used by client

	// Intercept -help and show usage screen
	flagHelp := flag.Bool("help", false, "Command Usage")

	flag.Parse()

	// CPU Profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		checkErr(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Default to localhost if no host or socket provided
	if *flagDbSock == "" && *flagDbHost == "" {
		*flagDbHost = "localhost"
	}

	dbi := mysqlCredentials{user: *flagDbUser, pass: *flagDbPass, host: *flagDbHost, port: *flagDbPort, sock: *flagDbSock}

	// Detect what functionality is being requested
	if *flagClient {
		if *flagServerHost == "" || *flagDbUser == "" {
			showUsage()
		} else {
			// Confirm mysql user exists
			mysqlUser, err := user.Lookup("mysql")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// Get mysql uid & gid
			dbi.uid, _ = strconv.Atoi(mysqlUser.Uid)
			dbi.gid, _ = strconv.Atoi(mysqlUser.Gid)

			runClient(*flagServerHost, *flagPort, *flagWorkers, &dbi)
		}
	} else if *flagDump {
		if *flagDbUser == "" {
			showUsage()
		} else {
			runDump(*flagDumpDir, &dbi)
		}
	} else if *flagServer {
		if *flagTablePath == "" || *flagBackupPath == "" {
			showUsage()
		} else {
			runServer(*flagTablePath, *flagBackupPath, *flagPort)
		}
	} else if *flagHelp {
		showUsage()
	} else {
		if len(flag.Args()) == 0 {
			showUsage()
		}
	}

	// Memory Profiling
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		checkErr(err)
		pprof.WriteHeapProfile(f)
		defer f.Close()
	}

	fmt.Println()
	fmt.Println("Total runtime =", time.Since(start))
}
