package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	_ "github.com/go-sql-driver/mysql" // Go MySQL driver
)

const mysqlTimeout = "3600"    // 1 hour - must be string
const mysqlWaitTimeout = "600" // 10 minutes - Prevent disconnect when dumping thousands of tables

type (
	// mysqlCredentials defines database connection information
	mysqlCredentials struct {
		user   string
		pass   string
		host   string
		port   string
		sock   string
		schema string
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

	// Determine tcp or socket connection
	var db *sql.DB
	var err error
	if dbi.sock != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@unix("+dbi.sock+")/"+dbi.schema+"?sql_log_bin=0&allowCleartextPasswords=1&tls=skip-verify&wait_timeout="+mysqlTimeout+"&net_write_timeout="+mysqlWaitTimeout)
		checkErr(err)
	} else if dbi.host != "" {
		db, err = sql.Open("mysql", dbi.user+":"+dbi.pass+"@tcp("+dbi.host+":"+dbi.port+")/"+dbi.schema+"?sql_log_bin=0&allowCleartextPasswords=1&tls=skip-verify&wait_timeout="+mysqlTimeout+"&net_write_timeout="+mysqlWaitTimeout)
		checkErr(err)
	}

	// Ping database to verify credentials
	err = db.Ping()

	return db, err
}
