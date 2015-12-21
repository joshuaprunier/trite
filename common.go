package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	_ "github.com/go-sql-driver/mysql" // Go MySQL driver
)

const (
	mysqlTimeout     = "3600" // 1 hour - must be string
	mysqlWaitTimeout = "600"  // 10 minutes - Prevent disconnect when dumping thousands of tables

	// Timeout length in seconds where ctrl+c is ignored.
	signalTimeout = 3
)

type (
	// mysqlCredentials defines database connection information
	mysqlCredentials struct {
		user   string
		pass   string
		host   string
		port   string
		sock   string
		schema string
		tls    bool
		uid    int
		gid    int
	}

	// CreateInfoStruct stores creation information for procedures, functions, triggers and views
	createInfoStruct struct {
		Name          string
		SqlMode       string
		Create        string
		CharsetClient string
		Collation     string
		DbCollation   string
	}
)

// checkErr is an error handling catch all function
func checkErr(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// ParseFileName splits a file name and returns two strings of the base and 3 digit extension
func parseFileName(text string) (string, string) {
	ext := strings.Split(text, ".")
	ext = ext[cap(ext)-1:]
	ret := ext[0]
	file := strings.TrimSuffix(text, "."+ret)

	return file, ret
}

// AddQuotes adds backtick quotes in cases where identifiers are all numeric or match reserved keywords
func addQuotes(s string) string {
	s = "`" + s + "`"
	return s
}

// connect returns a MySQL database connection handler
func (dbi *mysqlCredentials) connect() (*sql.DB, error) {
	// If password is blank prompt user
	if dbi.pass == "" {
		fmt.Println("Enter password: ")
		pwd, err := terminal.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			if err != io.EOF {
				checkErr(err)
			}
		}

		dbi.pass = string(pwd)
	}

	// Set MySQL driver parameters
	dbParameters := "sql_log_bin=0&wait_timeout=" + mysqlTimeout + "&net_write_timeout=" + mysqlWaitTimeout

	// Append cleartext and tls parameters if TLS is specified
	if dbi.tls == true {
		dbParameters = dbParameters + "&allowCleartextPasswords=1&tls=skip-verify"
	}

	// Determine tcp or socket connection
	var db *sql.DB
	var err error
	if dbi.sock != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@unix("+dbi.sock+")/"+dbi.schema+"?"+dbParameters)
		checkErr(err)
	} else if dbi.host != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@tcp("+dbi.host+":"+dbi.port+")/"+dbi.schema+"?"+dbParameters)
		checkErr(err)
	}

	// Ping database to verify credentials
	err = db.Ping()

	return db, err
}

// Catch signals
func catchNotifications() {
	state, err := terminal.GetState(int(os.Stdin.Fd()))
	checkErr(err)

	// Deal with SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	var timer time.Time
	go func() {
		for sig := range sigChan {
			// Prevent exiting on accidental signal send
			if time.Now().Sub(timer) < time.Second*signalTimeout {
				terminal.Restore(int(os.Stdin.Fd()), state)
				os.Exit(0)
			}

			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, sig, "signal caught!")
			fmt.Fprintf(os.Stderr, "Send signal again within %v seconds to exit\n", signalTimeout)
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "")

			timer = time.Now()
		}
	}()
}
