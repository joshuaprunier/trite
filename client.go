package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/joshuaprunier/mysqlUTF8"

	"golang.org/x/net/html"
)

// downloadInfoStruct stores information necessary for the client to download and apply objects to the database
type (
	clientConfigStruct struct {
		triteServerURL          string
		triteServerPort         string
		errorLogFile            string
		minDownloadProgressSize int64
	}

	downloadInfoStruct struct {
		db            *sql.DB
		taburl        string
		backurl       string
		schema        string
		table         string
		encodedSchema string
		encodedTable  string
		mysqldir      string
		uid           int
		gid           int
		engine        string
		extensions    []string
		triteFiles    []string
		version       string
		displayInfo   displayInfoStruct
		displayChan   chan displayInfoStruct
		wgApply       *sync.WaitGroup
	}

	displayInfoStruct struct {
		w       io.Writer
		fqTable string
		status  string
	}
)

const (
	mysqlPerms = 0660
)

var (
	displayTable    string
	errCount        int
	errApplyDrop    error
	errApplyCreate  error
	errApplyDiscard error
	errApplyLock    error
	errApplyRename  error
	errApplyImport  error
	errApplyAnalyze error
	errApplyUnlock  error
)

// startClient is responsible for retrieving database creation satements and binary table files from a trite server instance.
func startClient(clientConfig clientConfigStruct, dbi *mysqlCredentials) {
	// Make a database connection
	db, err := dbi.connect()
	defer db.Close()

	// Turn off idle connections
	db.SetMaxIdleConns(0)

	// Problem connecting to database
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Detect MySQL version and set import flag for 5.1 & 5.5
	var ignore string
	var version string
	err = db.QueryRow("show global variables like 'version'").Scan(&ignore, &version)
	checkErr(err)

	var importFlag string
	if strings.HasPrefix(version, "5.1") || strings.HasPrefix(version, "5.5") {
		err = db.QueryRow("show global variables like '%innodb%import%'").Scan(&importFlag, &ignore)
		checkErr(err)

		_, err = db.Exec("set global " + importFlag + "=1")
		checkErr(err)
	} else if strings.HasPrefix(version, "5.6") || strings.HasPrefix(version, "10") {
		// No import flag for 5.6 or MariaDB 10
	} else {
		fmt.Fprintln(os.Stderr, version, "is not supported")
		os.Exit(1)
	}

	// Get MySQL datadir
	var mysqldir string
	err = db.QueryRow("show variables like 'datadir'").Scan(&ignore, &mysqldir)
	checkErr(err)

	// Make sure mysql datadir is writable
	err = ioutil.WriteFile(mysqldir+"/trite_test", []byte("delete\n"), mysqlPerms)
	if err != nil {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "The MySQL data directory is not writable as this user!")
		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	} else {
		os.Remove(mysqldir + "/trite_test")
	}

	// URL variables
	taburl := "http://" + clientConfig.triteServerURL + ":" + clientConfig.triteServerPort + "/tables/"
	backurl := "http://" + clientConfig.triteServerURL + ":" + clientConfig.triteServerPort + "/backups/"

	// Verify server urls are accessible
	urls := []string{taburl, backurl}
	for _, url := range urls {
		_, err = http.Head(url)
		if err != nil {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "Problem connecting to", url)
			fmt.Fprintln(os.Stderr, "Check that the server is running, port number is correct or that a firewall is not blocking access")
			os.Exit(1)
		}
	}

	// Get a list of schemas from the trite server
	base, err := http.Get(taburl)
	checkHTTP(base, taburl)
	defer base.Body.Close()
	checkErr(err)

	schemas := parseAnchor(base)

	// Start up download workers
	var wgDownload sync.WaitGroup
	dl := make(chan downloadInfoStruct)
	go func() {
		for d := range dl {
			downloadTable(clientConfig, d)
			wgDownload.Done()
		}
	}()

	// Single thread display info from concurrent processes
	displayChan := make(chan displayInfoStruct)
	go display(displayChan)

	// Apply wait group
	var wgApply sync.WaitGroup

	// Loop through all schemas and apply tables
	for _, schema := range schemas {
		// Check if schema exists
		checkSchema(db, schema, taburl+path.Join(schema, schema+sqlExtension))

		// Parse html and get a list of tables to transport
		tablesDir, err := http.Get(taburl + path.Join(schema, "tables"))
		checkHTTP(tablesDir, taburl+path.Join(schema, "tables"))
		defer tablesDir.Body.Close()
		checkErr(err)
		tables := parseAnchor(tablesDir)

		// ignore when path is empty
		if len(tables) > 0 {
			for _, table := range tables {
				wgDownload.Add(1)
				wgApply.Add(1)
				downloadInfo := downloadInfoStruct{
					db:          db,
					taburl:      taburl,
					backurl:     backurl,
					schema:      schema,
					table:       table[:len(table)-4],
					mysqldir:    mysqldir,
					uid:         dbi.uid,
					gid:         dbi.gid,
					version:     version,
					displayChan: displayChan,
					wgApply:     &wgApply,
				}

				// Do filename encoding for schema and table if needed
				if mysqlUTF8.NeedsEncoding(downloadInfo.schema) {
					downloadInfo.encodedSchema = mysqlUTF8.EncodeFilename(downloadInfo.schema)
				}
				if mysqlUTF8.NeedsEncoding(downloadInfo.table) {
					downloadInfo.encodedTable = mysqlUTF8.EncodeFilename(downloadInfo.table)
				}

				// Send downloadInfo into channel and begin download
				dl <- downloadInfo
			}
		}
	}
	wgDownload.Wait()
	wgApply.Wait()

	// Loop through all schemas again and apply triggers, views, procedures & functions
	time.Sleep(1 * time.Millisecond)
	fmt.Println()
	objectTypes := []string{"trigger", "view", "procedure", "function"}
	for _, schema := range schemas {
		for _, objectType := range objectTypes {
			applyObjects(db, objectType, schema, taburl)
		}
	}

	// Reset global db variables
	if importFlag != "" {
		_, err = db.Exec("set global " + importFlag + "=0")
	}

	errCount := getErrCount()
	if errCount > 0 {
		fmt.Println()
		fmt.Println("! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ")
		fmt.Println(errCount, "errors were encountered")
		fmt.Println("Check", clientConfig.errorLogFile, "for more details")
		fmt.Println("! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ")
	}
}

// getErrCount returns the number of errors encountered
func getErrCount() int {
	return errCount
}

// incErrCount increases the error count
func incErrCount() {
	var mu sync.Mutex

	mu.Lock()
	errCount++
	mu.Unlock()
}

// getDisplayTable returns the current table name being displayed
func getDisplayTable() string {
	return displayTable
}

// setDisplayTable sets the current table name being displayed
func setDisplayTable(table string) {
	var mu sync.Mutex

	mu.Lock()
	displayTable = table
	mu.Unlock()
}

// checkHTTP causes the program to exit if a http get request does not return a 200
func checkHTTP(r *http.Response, url string) {
	if r.StatusCode != 200 {
		fmt.Println(r.StatusCode, "returned from:", url)
		os.Exit(1)
	}
}

// parseAnchor returns a string slice list of objects from an http.FileServer(). Trailing forward slashes from directories are removed.
func parseAnchor(r *http.Response) []string {
	txt := make([]string, 0)
	tok := html.NewTokenizer(r.Body)

	for {
		tt := tok.Next()
		if tt == html.ErrorToken {
			break
		}

		if tt == html.TextToken {
			a := tok.Raw()
			if a[0] != 10 {
				txt = append(txt, string(bytes.Trim(a, "/")))
			}
		}
	}
	return txt
}

// checkSchema creates a schema if it does not already exist
func checkSchema(db *sql.DB, schema string, schemaCreateURL string) {
	var exists string
	err := db.QueryRow("show databases like '" + schema + "'").Scan(&exists)

	if err != nil {
		resp, err := http.Get(schemaCreateURL)
		checkHTTP(resp, schemaCreateURL)
		defer resp.Body.Close()
		checkErr(err)

		stmt, _ := ioutil.ReadAll(resp.Body)
		_, err = db.Exec(string(stmt))
		checkErr(err)
	}
}

// display receives display events and queues events to make printing sane
func display(displayChan chan displayInfoStruct) {
	var lastDisplayLength int
	var currentDisplay displayInfoStruct
	displayQueue := make([]displayInfoStruct, 0)

	// Receive channel display events
	for displayInfo := range displayChan {
		if currentDisplay.fqTable == "" {
			currentDisplay = displayInfo
		}

		// Set current display table
		if getDisplayTable() == "" && currentDisplay.status == "Downloading" {
			setDisplayTable(currentDisplay.fqTable)
		}

		// If the channel event is for the current table update the display otherwise add it to the queue
		if currentDisplay.fqTable == displayInfo.fqTable {
			// Blank out the previous status and display new status
			fmt.Fprintf(displayInfo.w, strings.Repeat(" ", lastDisplayLength)+"\r")
			line := fmt.Sprintf("%s: %s", displayInfo.status, displayInfo.fqTable)
			lastDisplayLength = len(line)
			fmt.Fprintf(displayInfo.w, line+"\r")

			// Decide what to do when receiving a tables final status
			if displayInfo.status == "Restored" || displayInfo.status == "ERROR" {
				fmt.Fprintf(displayInfo.w, "\n")
				// Blank current table variable if queue is empty otherwise display queued events
				if len(displayQueue) == 0 {
					currentDisplay.fqTable = ""
				} else {
					tmpQueue := make([]displayInfoStruct, 0)
					for i := 0; i < len(displayQueue); i++ {
						if displayQueue[i].status == "Restored" || displayQueue[i].status == "ERROR" {
							line := fmt.Sprintf("%s: %s", displayQueue[i].status, displayQueue[i].fqTable)
							fmt.Fprintf(displayInfo.w, line+"\n")
						} else if displayQueue[i].fqTable != currentDisplay.fqTable {
							tmpQueue = append(tmpQueue, displayQueue[i])
						}
					}

					// Set current table variable to oldest queue entry or blank the current table variable if queue is empty
					if len(tmpQueue) > 0 {
						displayQueue = tmpQueue
						currentDisplay = displayQueue[0]

						// Set current display table
						if currentDisplay.status == "Downloading" {
							setDisplayTable(currentDisplay.fqTable)
						}

						// Oldest queue item is now current table so display the status
						line := fmt.Sprintf("%s: %s", currentDisplay.status, currentDisplay.fqTable)
						lastDisplayLength = len(line)
						fmt.Fprintf(currentDisplay.w, line+"\r")
					} else {
						currentDisplay.fqTable = ""
						setDisplayTable(currentDisplay.fqTable)
					}
				}
			}
		} else {
			// Add the table event to the queue, update the status if the table is already in the queue
			if len(displayQueue) == 0 {
				displayQueue = append(displayQueue, displayInfo)
			} else {
				var tableInQueue bool
				for i := 0; i < len(displayQueue); i++ {
					if displayQueue[i].fqTable == displayInfo.fqTable {
						displayQueue[i] = displayInfo
						tableInQueue = true
					}
				}

				if !tableInQueue {
					displayQueue = append(displayQueue, displayInfo)
				}
			}
		}
	}
}

// downloadTables retrieves files from the HTTP server. Files to download is MySQL engine specific.
func downloadTable(clientConfig clientConfigStruct, downloadInfo downloadInfoStruct) {
	downloadInfo.displayInfo.w = os.Stdout
	downloadInfo.displayInfo.fqTable = downloadInfo.schema + "." + downloadInfo.table
	downloadInfo.displayInfo.status = "Downloading"
	downloadInfo.displayChan <- downloadInfo.displayInfo

	// Use encoded schema and table if present
	var schemaFilename string
	var tableFilename string
	if downloadInfo.encodedSchema != "" {
		schemaFilename = downloadInfo.encodedSchema
	} else {
		schemaFilename = downloadInfo.schema
	}

	if downloadInfo.encodedTable != "" {
		tableFilename = downloadInfo.encodedTable
	} else {
		tableFilename = downloadInfo.table
	}

	// Ensure backup exists and check the engine type
	// Assume InnoDB first
	resp, err := http.Head(downloadInfo.backurl + path.Join(schemaFilename, tableFilename+".ibd"))
	checkErr(err)

	var engine string
	extensions := make([]string, 0)
	if resp.StatusCode == 200 {
		engine = "InnoDB"

		// 5.1 & 5.5 use .exp - 5.6 uses .cfg but it is ignored. Metadata checks appeared too brittle in testing.
		if strings.HasPrefix(downloadInfo.version, "5.1") || strings.HasPrefix(downloadInfo.version, "5.5") {
			extensions = append(extensions, ".exp")
		}

		extensions = append(extensions, ".ibd")
	} else {
		// Check for MyISAM
		resp, err := http.Head(downloadInfo.backurl + path.Join(schemaFilename, tableFilename+".MYD"))
		checkErr(err)

		if resp.StatusCode == 200 {
			engine = "MyISAM"
			extensions = append(extensions, ".MYI")
			extensions = append(extensions, ".MYD")
			extensions = append(extensions, ".frm")
		} else {
			fmt.Println()
			fmt.Println("!!!!!!!!!!!!!!!!!!!!")
			fmt.Println("The .ibd or .MYD file is missing for table", downloadInfo.table)
			fmt.Println("Skipping ...")
			fmt.Println("!!!!!!!!!!!!!!!!!!!!")
			fmt.Println()

			return
		}
	}

	// Update downloadInfo struct with engine type and extensions array
	downloadInfo.engine = engine
	downloadInfo.extensions = extensions

	// Loop through and download all files from extensions array
	triteFiles := make([]string, 0)
	for _, extension := range extensions {
		triteFile := filepath.Join(downloadInfo.mysqldir, schemaFilename, tableFilename+extension+".trite")
		urlfile := downloadInfo.backurl + path.Join(schemaFilename, tableFilename+extension)

		// Ensure the .exp exists if we expect it
		// Checking this due to a bug encountered where XtraBackup did not create a tables .exp file
		if extension == ".exp" {
			resp, err := http.Head(downloadInfo.backurl + path.Join(schemaFilename, tableFilename+".exp"))
			checkHTTP(resp, downloadInfo.backurl+path.Join(schemaFilename, tableFilename+".exp"))
			checkErr(err)

			if resp.StatusCode != 200 {
				fmt.Println()
				fmt.Println("!!!!!!!!!!!!!!!!!!!!")
				fmt.Println("The .exp file is missing for table", downloadInfo.table)
				fmt.Println("Skipping ...")
				fmt.Println("!!!!!!!!!!!!!!!!!!!!")
				fmt.Println()

				return
			}
		}

		// Request and write file
		fo, err := os.Create(triteFile)
		checkErr(err)
		defer fo.Close()

		// Chown to mysql user
		os.Chown(triteFile, downloadInfo.uid, downloadInfo.gid)
		os.Chmod(triteFile, mysqlPerms)

		// Download files from trite server
		w := bufio.NewWriter(fo)
		ibdresp, err := http.Get(urlfile)
		checkHTTP(ibdresp, urlfile)
		defer ibdresp.Body.Close()
		checkErr(err)
		sizeServer := ibdresp.ContentLength

		var sizeDown int64
		if extension != ".exp" && sizeServer > clientConfig.minDownloadProgressSize*1073741824 {
			progressReader := &reader{
				reader:     ibdresp.Body,
				size:       ibdresp.ContentLength,
				drawFunc:   drawTerminalf(downloadInfo.displayInfo.w, drawTextFormatPercent),
				drawPrefix: "Downloading: " + downloadInfo.schema + "." + downloadInfo.table,
			}
			sizeDown, err = w.ReadFrom(progressReader)
		} else {
			sizeDown, err = w.ReadFrom(ibdresp.Body)
		}
		checkErr(err)
		w.Flush()

		// Check if size of file downloaded matches size on server -- Add retry ability
		if sizeDown != sizeServer {
			fmt.Println("\n\nFile download size does not match size on server!")
			fmt.Println(triteFile, "has been removed.")

			// Remove partial file download
			err = os.Remove(triteFile)
			checkErr(err)
		}

		triteFiles = append(triteFiles, triteFile)
	}

	downloadInfo.triteFiles = triteFiles

	// Call applyTables
	go applyTables(clientConfig, &downloadInfo)
}

// applyTables performs all of the database actions required to restore a table
func applyTables(clientConfig clientConfigStruct, downloadInfo *downloadInfoStruct) {
	downloadInfo.displayInfo.status = "Applying"
	downloadInfo.displayChan <- downloadInfo.displayInfo

	// Start db transaction
	tx, err := downloadInfo.db.Begin()
	checkErr(err)

	// make the following code work for any settings -- need to preserve before changing so they can be changed back, figure out global vs session and how to handle not setting properly
	_, err = tx.Exec("set session foreign_key_checks=0")
	_, err = tx.Exec("set session lock_wait_timeout=60")
	_, err = tx.Exec("use " + addQuotes(downloadInfo.schema))

	switch downloadInfo.engine {
	case "InnoDB":
		// Get table create
		resp, err := http.Get(downloadInfo.taburl + path.Join(downloadInfo.schema, "tables", downloadInfo.table+sqlExtension))
		checkHTTP(resp, downloadInfo.taburl+path.Join(downloadInfo.schema, "tables", downloadInfo.table+sqlExtension))
		defer resp.Body.Close()
		checkErr(err)
		stmt, _ := ioutil.ReadAll(resp.Body)

		// Drop table if exists
		_, err = tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		if err != nil {
			errApplyDrop = fmt.Errorf("There was an error dropping table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyDrop)

			return
		}

		// Create table
		_, err = tx.Exec(string(stmt))
		if err != nil {
			errApplyCreate = fmt.Errorf("There was an error creating table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyCreate)

			return
		}

		// Discard the tablespace
		_, err = tx.Exec("alter table " + addQuotes(downloadInfo.table) + " discard tablespace")
		if err != nil {
			errApplyDiscard = fmt.Errorf("There was an error discarding the tablespace for %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyDiscard)

			return
		}

		// Lock the table just in case
		_, err = tx.Exec("lock table " + addQuotes(downloadInfo.table) + " write")
		if err != nil {
			errApplyLock = fmt.Errorf("There was an error locking table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyLock)

			return
		}

		// Rename trite download files
		for _, triteFile := range downloadInfo.triteFiles {
			err := os.Rename(triteFile, triteFile[:len(triteFile)-6])
			if err != nil {
				errApplyRename = fmt.Errorf("There was an error renaming table %s.%s", downloadInfo.schema, downloadInfo.table)
				handleApplyError(tx, clientConfig, downloadInfo, errApplyRename)

				return
			}

		}

		// Import the tablespace
		_, err = tx.Exec("alter table " + addQuotes(downloadInfo.table) + " import tablespace")
		if err != nil {
			errApplyImport = fmt.Errorf("There was an error importing the tablespace for %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyImport)

			return
		}

		// Analyze the table otherwise there will be no index statistics
		_, err = tx.Exec("analyze local table " + addQuotes(downloadInfo.table))
		if err != nil {
			errApplyAnalyze = fmt.Errorf("There was an error analyzing table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyAnalyze)

			return
		}

		// Unlock the table
		_, err = tx.Exec("unlock tables")
		if err != nil {
			errApplyUnlock = fmt.Errorf("There was an error unlocking table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyUnlock)

			return
		}

		// Commit transaction
		err = tx.Commit()
		checkErr(err)

	case "MyISAM":
		// Drop table if exists
		_, err := tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		if err != nil {
			errApplyDrop = fmt.Errorf("There was an error dropping table %s.%s", downloadInfo.schema, downloadInfo.table)
			handleApplyError(tx, clientConfig, downloadInfo, errApplyDrop)

			return
		}

		// Rename happens here
		for _, triteFile := range downloadInfo.triteFiles {
			err := os.Rename(triteFile, triteFile[:len(triteFile)-6])
			if err != nil {
				errApplyRename = fmt.Errorf("There was an error renaming table %s.%s", downloadInfo.schema, downloadInfo.table)
				handleApplyError(tx, clientConfig, downloadInfo, errApplyRename)

				return
			}
		}

		// Commit transaction
		err = tx.Commit()
		checkErr(err)

	default:
		fmt.Fprintln(os.Stderr, "\t*", "Backup does not exist or", downloadInfo.table, "is using an engine other than InnoDB or MyISAM")
		fmt.Fprintln(os.Stderr, "\t*", "Skipping")
	}

	downloadInfo.displayInfo.status = "Restored"
	downloadInfo.displayChan <- downloadInfo.displayInfo

	downloadInfo.wgApply.Done()
}

// handleApplyError deals with rollback, logging and notification of errors that may occur during the apply phase
func handleApplyError(tx *sql.Tx, clientConfig clientConfigStruct, downloadInfo *downloadInfoStruct, applyErr error) {

	// Write innodb status and processlist to error log
	var ignore1 string
	var ignore2 string
	var innodbStatus string
	err := tx.QueryRow("show engine innodb status").Scan(&ignore1, &ignore2, &innodbStatus)
	checkErr(err)

	var id string
	var user string
	var host string
	var database string
	var command string
	var time string
	var state string
	var info string

	rows, err := tx.Query("select id, user, host, ifnull(db,'NULL'), command, time, ifnull(state,'NULL'), ifnull(info,'NULL') from information_schema.processlist where id != connection_id()")
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	// Log the error
	var f *os.File
	f, err = os.OpenFile(clientConfig.errorLogFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		f, err = os.OpenFile(clientConfig.errorLogFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		checkErr(err)
	}

	l := log.New(f, "", log.LstdFlags)
	l.Println(applyErr)
	l.Println(innodbStatus)

	// Print a few blank lines to separate the innodb status and processlist
	for i := 0; i < 3; i++ {
		l.Println()
	}

	// Tabwriter to make the processlist more readable
	tw := new(tabwriter.Writer)
	tw.Init(f, 0, 8, 1, ' ', tabwriter.Debug)
	fmt.Fprintln(tw, "id\tuser\thost\tdatabase\tcommand\ttime\tstate\tinfo")
	for rows.Next() {
		err = rows.Scan(&id, &user, &host, &database, &command, &time, &state, &info)
		if err != nil {
			fmt.Println("ERROR:", err)
		}

		fmt.Fprintln(tw, id, "\t", user, "\t", host, "\t", database, "\t", command, "\t", time, "\t", state, "\t", info)
	}
	tw.Flush()

	// Print a few blank lines to separate errors
	for i := 0; i < 10; i++ {
		l.Println()
	}

	f.Close()

	// Handle rollback and cleanup depending on the error
	switch applyErr {
	case errApplyDrop:
		for _, triteFile := range downloadInfo.triteFiles {
			os.Remove(triteFile)
		}
		tx.Rollback()

	case errApplyCreate:
		for _, triteFile := range downloadInfo.triteFiles {
			os.Remove(triteFile)
		}
		tx.Rollback()

	case errApplyDiscard:
		for _, triteFile := range downloadInfo.triteFiles {
			os.Remove(triteFile)
		}
		tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		tx.Rollback()

	case errApplyLock:
		for _, triteFile := range downloadInfo.triteFiles {
			os.Remove(triteFile)
		}
		tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		tx.Rollback()

	case errApplyRename:
		for _, triteFile := range downloadInfo.triteFiles {
			os.Remove(triteFile)
		}
		tx.Exec("unlock tables")
		tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		tx.Rollback()

	case errApplyImport:
		tx.Exec("unlock tables")
		tx.Exec("drop table if exists " + addQuotes(downloadInfo.table))
		tx.Rollback()

	case errApplyAnalyze:
		tx.Exec("unlock tables")
		tx.Rollback()

	case errApplyUnlock:
		tx.Rollback()
	}

	incErrCount()

	// Send error status to display
	downloadInfo.displayInfo.status = "ERROR"
	downloadInfo.displayChan <- downloadInfo.displayInfo
	downloadInfo.wgApply.Done()
}

// applyObjects is a generic function for creating procedures, functions, views and triggers.
func applyObjects(db *sql.DB, objectType string, schema string, taburl string) {
	objectTypePlural := objectType + "s"

	// Start transaction
	tx, err := db.Begin()
	checkErr(err)

	// Use schema
	_, err = tx.Exec("set session foreign_key_checks=0")
	_, err = tx.Exec("use " + schema)

	// Get a list of objects to create
	loc, err := http.Get(taburl + path.Join(schema, objectTypePlural))
	checkHTTP(loc, taburl+path.Join(schema, objectTypePlural))
	defer loc.Body.Close()
	checkErr(err)
	objects := parseAnchor(loc)
	fmt.Println("Applying", objectTypePlural, "for", schema)

	// Only continue if there are objects to create
	if len(objects) > 0 {
		for _, object := range objects {

			objectName, _ := parseFileName(object)
			_, err := tx.Exec("drop " + objectType + " if exists " + addQuotes(objectName))
			resp, err := http.Get(taburl + path.Join(schema, objectTypePlural, object))
			checkHTTP(resp, taburl+path.Join(schema, objectTypePlural, object))
			defer resp.Body.Close()
			checkErr(err)
			stmt, _ := ioutil.ReadAll(resp.Body)

			var objInfo createInfoStruct
			err = json.Unmarshal(stmt, &objInfo)
			checkErr(err)

			// Set session level variables to recreate stored code properly
			if objInfo.SqlMode != "" {
				_, err = tx.Exec("set session sql_mode = '" + objInfo.SqlMode + "'")
			}
			if objInfo.CharsetClient != "" {
				_, err = tx.Exec("set session character_set_client = '" + objInfo.CharsetClient + "'")
			}
			if objInfo.Collation != "" {
				_, err = tx.Exec("set session collation_connection = '" + objInfo.Collation + "'")
			}
			if objInfo.DbCollation != "" {
				_, err = tx.Exec("set session collation_database = '" + objInfo.DbCollation + "'")
			}

			// Create object
			_, err = tx.Exec(objInfo.Create)
			checkErr(err)

		}
	}

	// Commit transaction
	err = tx.Commit()
	checkErr(err)
}
