package common

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

// Type definitions
type (
	// DbInfoStruct defines database connection information
	DbInfoStruct struct {
		User   string
		Pass   string
		Host   string
		Port   string
		Sock   string
		Schema string
		UID    int
		GID    int
	}

	// CreateInfoStruct stores creation information for procedures, functions, triggers and views
	CreateInfoStruct struct {
		Name          string
		SqlMode       string
		Create        string
		CharsetClient string
		Collation     string
		DbCollation   string
	}
)

// CheckErr is an error handling catch all. Frequent errors that come through here should be handled in the specific portion of the code where they originate.
func CheckErr(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// ParseFileName splits a file name and returns two strings of the base and 3 digit extension
func ParseFileName(text string) (string, string) {
	ext := strings.Split(text, ".")
	ext = ext[cap(ext)-1:]
	ret := ext[0]
	file := strings.TrimSuffix(text, "."+ret)

	return file, ret
}

// AddQuotes adds backtick quotes in cases where identifiers are all numeric or match reserved keywords
func AddQuotes(s string) string {
	s = "`" + s + "`"
	return s
}

// DbConn returns a db connection pointer, do some detection if we should connect as localhost(client) or tcp(dump). Localhost is to hopefully support protected db mode with skip networking. Utf8 character set hardcoded for all connections. Transaction control is left up to other worker functions.
func DbConn(dbInfo *DbInfoStruct) (*sql.DB, error) {
	// If password is blank prompt user - Not perfect as it prints the password typed to the screen
	if dbInfo.Pass == "" {
		fmt.Println("Enter password: ")
		pwd, err := ReadPassword(0)
		CheckErr(err)
		dbInfo.Pass = string(pwd)
	}

	// Determine tcp or socket connection
	var db *sql.DB
	var err error
	if dbInfo.Sock != "" {
		db, err = sql.Open("mysql", dbInfo.User+":"+dbInfo.Pass+"@unix("+dbInfo.Sock+")/"+dbInfo.Schema+"?sql_log_bin=0&allowCleartextPasswords=1&tls=skip-verify&wait_timeout="+mysqlTimeout+"&net_write_timeout="+mysqlWaitTimeout)
		CheckErr(err)
	} else if dbInfo.Host != "" {
		db, err = sql.Open("mysql", dbInfo.User+":"+dbInfo.Pass+"@tcp("+dbInfo.Host+":"+dbInfo.Port+")/"+dbInfo.Schema+"?sql_log_bin=0&allowCleartextPasswords=1&tls=skip-verify&wait_timeout="+mysqlTimeout+"&net_write_timeout="+mysqlWaitTimeout)
		CheckErr(err)
	} else {
		fmt.Println("should be no else")
	}

	// Ping database to verify credentials
	err = db.Ping()

	return db, err
}

// ReadPassword is borrowed from the crypto/ssh/terminal sub repo to accept a password from stdin without local echo.
// http://godoc.org/code.google.com/p/go.crypto/ssh/terminal#Terminal.ReadPassword
func ReadPassword(fd int) ([]byte, error) {
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
