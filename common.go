package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"syscall"
	"unsafe"

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
		pwd, err := readPassword(0)
		checkErr(err)
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

// ReadPassword is borrowed from the crypto/ssh/terminal sub repo to accept a password from stdin without local echo.
// http://godoc.org/code.google.com/p/go.crypto/ssh/terminal#Terminal.ReadPassword
func readPassword(fd int) ([]byte, error) {
	var oldState syscall.Termios
	if _, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0); err != 0 {
		return nil, err
	}

	newState := oldState
	newState.Lflag &^= syscall.ECHO
	newState.Lflag |= syscall.ICANON | syscall.ISIG
	newState.Iflag |= syscall.ICRNL
	if _, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&newState)), 0, 0, 0); err != 0 {
		return nil, err
	}

	defer func() {
		syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0)
	}()

	var buf [16]byte
	var ret []byte
	for {
		n, err := syscall.Read(fd, buf[:])
		if err != nil {
			return nil, err
		}

		if n == 0 {
			if len(ret) == 0 {
				return nil, io.EOF
			}
			break
		}

		if buf[n-1] == '\n' {
			n--
		}

		ret = append(ret, buf[:n]...)
		if n < len(buf) {
			break
		}
	}

	return ret, nil
}
