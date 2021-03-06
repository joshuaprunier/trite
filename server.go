package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // http server profiling
	"os"
	"strings"

	"github.com/klauspost/pgzip"
)

// startServer receives a port number and a directory path for create definitions output by trite in dump mode and another directory path with an xtrabackup processed with the --export flag
func startServer(tablePath string, backupPath string, port string) {
	// Make sure directory passed in has trailing slash
	if strings.HasSuffix(backupPath, "/") == false {
		backupPath = backupPath + "/"
	}

	// Ensure the backup has been prepared for transporting with --export
	check := verifyBackup(backupPath, false)
	if check == false {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "It appears that --export has not be run on your backups!")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	}

	// Start HTTP server listener
	fmt.Println()
	fmt.Println("Starting server listening on port", port)
	http.HandleFunc("/", rootHandler)
	http.Handle("/tables/", http.StripPrefix("/tables/", http.FileServer(http.Dir(tablePath))))
	http.Handle("/backups/", http.StripPrefix("/backups/", http.FileServer(http.Dir(backupPath))))
	http.Handle("/gz/", http.StripPrefix("/gz/", gzHandler(http.FileServer(http.Dir(backupPath)))))
	err := http.ListenAndServe(":"+port, nil)

	// Check if port is already in use
	if err != nil {
		if err.Error() == "listen tcp :"+port+": bind: address already in use" {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "ERROR: Port", port, "is already in use!")
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr)
			os.Exit(1)
		} else {
			checkErr(err)
		}
	}
}

// verifyBackup traverses the backup directory and confirms there are .exp files which is proof --export was run
func verifyBackup(dir string, flag bool) bool {
	files, ferr := ioutil.ReadDir(dir)
	checkErr(ferr)
	for _, file := range files {
		// Check if file has a .exp extension, that means --export has been performed on the backup
		_, ext := parseFileName(file.Name())

		// Recursive function call for subdirectories
		if file.IsDir() {
			flag := verifyBackup(dir+file.Name()+"/", flag)
			if flag == true {
				return flag
			}
		} else {
			if ext == "exp" {
				flag = true
				break
			}
		}
	}
	return flag
}

// rootHandler is a convenience landing page with links to the dump & backup files
func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `
	<html>
		<head>
			<title>TRITE</title>
		</head>
		<body>
			<a href="/tables">tables</a>
			<br>
			<a href="/backups">backups</a>
		</body>
	</html>
	`)
}

type gzResponseWriter struct {
	http.ResponseWriter
	io.Writer
}

func (w gzResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func gzHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "identity")
		gz, err := pgzip.NewWriterLevel(w, pgzip.BestCompression)
		checkErr(err)
		defer gz.Close()
		h.ServeHTTP(gzResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}
