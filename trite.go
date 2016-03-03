package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"runtime"
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
    -gz: Compress xtraBackup files for downloading across slower networks (default false)

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

	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	// Profiling flags
	var cpuprofile = f.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = f.String("memprofile", "", "write memory profile to this file")

	// MySQL flags
	flagDbUser := f.String("user", "", "MySQL username")
	flagDbPass := f.String("pass", "", "MySQL password")
	flagDbHost := f.String("host", "", "MySQL host")
	flagDbPort := f.String("port", "3306", "MySQL port")
	flagDbSock := f.String("socket", "", "MySQL socket")
	flagDbTLS := f.Bool("tls", false, "Enable TLS & cleartext passwords")

	// Client flags
	flagClient := f.Bool("client", false, "Run client")
	flagTriteServer := f.String("triteServer", "", "Hostname of the trite server")
	flagTriteMaxConnections := f.Int("triteMaxConnections", 20, "Max concurrent trite db connections")
	flagErrorLog := f.String("errorLog", wd+"/trite.err", "Error log file path")
	flagProgressLimit := f.Int64("progressLimit", 5, "Progress will not be displayed for files smaller than progressLimit")
	flagGz := f.Bool("gz", false, "Use the servers gz endpoint to download compressed files")

	// Dump flags
	flagDump := f.Bool("dump", false, "Run dump")
	flagDumpDir := f.String("dumpDir", wd, "Directory for output")

	// Server flags
	flagServer := f.Bool("server", false, "Run server")
	flagDumpPath := f.String("dumpPath", "", "Path to create statement dump files")
	flagBackupPath := f.String("backupPath", "", "Path to database backup files")
	flagTritePort := f.String("tritePort", "12000", "Trite server port number")

	// Intercept -help and show usage screen
	flagHelp := f.Bool("help", false, "Command Usage")

	f.SetOutput(ioutil.Discard)

	err = f.Parse(os.Args[1:])
	if err != nil {
		fmt.Println(err)
		showUsage()
		os.Exit(0)
	}

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

	dbi := mysqlCredentials{user: *flagDbUser, pass: *flagDbPass, host: *flagDbHost, port: *flagDbPort, sock: *flagDbSock, tls: *flagDbTLS}

	// Detect what functionality is being requested
	if *flagClient {
		if *flagTriteServer == "" || *flagDbUser == "" {
			showUsage()
		} else {
			if runtime.GOOS != "windows" {
				// Confirm mysql user exists
				mysqlUser, err := user.Lookup("mysql")
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}

				// Get mysql uid & gid
				dbi.uid, _ = strconv.Atoi(mysqlUser.Uid)
				dbi.gid, _ = strconv.Atoi(mysqlUser.Gid)
			}

			cliConfig := clientConfigStruct{triteServerURL: *flagTriteServer, triteServerPort: *flagTritePort, triteMaxConnections: *flagTriteMaxConnections, errorLogFile: *flagErrorLog, minDownloadProgressSize: *flagProgressLimit, gz: *flagGz}

			startClient(cliConfig, &dbi)
		}
	} else if *flagDump {
		if *flagDbUser == "" {
			showUsage()
		} else {
			startDump(*flagDumpDir, &dbi)
		}
	} else if *flagServer {
		if *flagDumpPath == "" || *flagBackupPath == "" {
			showUsage()
		} else {
			startServer(*flagDumpPath, *flagBackupPath, *flagTritePort)
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
