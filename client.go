package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/html"
)

const mysqlPerms = 0660

// downloadInfoStruct stores information necessary for the client to download and apply objects to the database
type (
	downloadInfoStruct struct {
		taburl     string
		backurl    string
		schema     string
		table      string
		mysqldir   string
		uid        int
		gid        int
		engine     string
		extensions []string
		version    string
	}
)

// startClient is responsible for retrieving database creation satements and binary table files from a trite server instance.
func startClient(url string, port string, workers uint, dbi *mysqlCredentials) {

	// Make a database connection
	db, err := dbi.connect()
	defer db.Close()

	// Problem connecting to database
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Percona import variable differs between versions
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
	taburl := "http://" + url + ":" + port + "/tables/"
	backurl := "http://" + url + ":" + port + "/backups/"

	// Verify server urls are accessible
	_, err = http.Head(taburl)
	if err != nil {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Problem connecting to", taburl)
		fmt.Fprintln(os.Stderr, "Check that the server is running, port number is correct or that a firewall is not blocking access")
		os.Exit(1)
	}

	_, err = http.Head(backurl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Problem connecting to", backurl)
		fmt.Fprintln(os.Stderr, "Check that the server is running, port number is correct or that a firewall is not blocking access")
		os.Exit(1)
	}

	// Parse html and get a list of schemas to transport
	base := getURL(taburl)
	defer base.Body.Close()
	schemas := parseAnchor(base)

	// Loop through all schemas and apply tables
	var active int32 //limit number of concurrent running applyTables()
	wg := new(sync.WaitGroup)
	for _, schema := range schemas {

		// Check if schema exists
		schemaTrimmed := strings.Trim(schema, "/")
		checkSchema(db, schemaTrimmed, taburl+schema+schemaTrimmed+".sql")

		// Parse html and get a list of tables to transport
		tablesDir := getURL(taburl + schema + "/tables")
		defer tablesDir.Body.Close()
		tables := parseAnchor(tablesDir)

		if len(tables) > 0 { // ignore when path is empty
			for _, table := range tables {

				downloadInfo := downloadInfoStruct{taburl: taburl, backurl: backurl, schema: schema, table: table, mysqldir: mysqldir, uid: dbi.uid, gid: dbi.gid, version: version}

				// Infinite loop to keep active go routines to 5 or less
				for {
					if active < int32(workers) {
						break
					} else {
						time.Sleep(1 * time.Second)
					}
				}

				wg.Add(1)
				atomic.AddInt32(&active, 1)
				go downloadTable(db, downloadInfo, &active, wg)
			}
		}
	}
	wg.Wait()

	// Loop through all schemas again and apply triggers, views, procedures & functions
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
}

// getURL is a small http.Get() wrapper
func getURL(u string) *http.Response {
	resp, err := http.Get(u)
	checkErr(err)

	return resp
}

// parseAnchor returns a slice of files and directories from a HTTP response. This function requires the google net/html sub repo.
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
				txt = append(txt, string(a))
			}
		}
	}
	return txt
}

// checkSchema creates a schema if it does not already exist
func checkSchema(db *sql.DB, schema string, url string) {
	var exists string
	err := db.QueryRow("show databases like '" + schema + "'").Scan(&exists)

	if err != nil {
		resp := getURL(url)
		defer resp.Body.Close()
		stmt, _ := ioutil.ReadAll(resp.Body)
		_, err = db.Exec(string(stmt))
		checkErr(err)

		fmt.Println("	Created schema", schema)
	}
}

// downloadTables retrieves files from the HTTP server. Files to download is MySQL engine specific.
func downloadTable(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
	filename, _ := parseFileName(downloadInfo.table)

	// Ensure backup exists and check the engine type
	// Make separate function to determine engine type
	resp, err := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".ibd") // Assume InnoDB first
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
		resp, err := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".MYD") // Check for MyISAM
		checkErr(err)

		if resp.StatusCode == 200 {
			engine = "MyISAM"
			extensions = append(extensions, ".MYI")
			extensions = append(extensions, ".MYD")
			extensions = append(extensions, ".frm")
		} else {
			fmt.Println()
			fmt.Println("!!!!!!!!!!!!!!!!!!!!")
			fmt.Println("The .ibd or .MYD file is missing for table", filename)
			fmt.Println("Skipping ...")
			fmt.Println("!!!!!!!!!!!!!!!!!!!!")
			fmt.Println()

			// Need to decrement since applyTables() will never be called
			atomic.AddInt32(active, -1)
			wg.Done()

			return
		}
	}

	// Update downloadInfo struct with engine type and extensions array
	downloadInfo.engine = engine
	downloadInfo.extensions = extensions

	// Loop through and download all files from extensions array
	for _, extension := range extensions {
		tmpfile := downloadInfo.mysqldir + downloadInfo.schema + filename + extension + ".trite"
		urlfile := downloadInfo.backurl + downloadInfo.schema + filename + extension

		// Ensure the .exp exists if we expect it
		// Checking this due to a bug encountered where XtraBackup did not create a tables .exp file
		if extension == ".exp" {
			resp, err := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".exp")
			checkErr(err)

			if resp.StatusCode != 200 {
				fmt.Println()
				fmt.Println("!!!!!!!!!!!!!!!!!!!!")
				fmt.Println("The .exp file is missing for table", filename)
				fmt.Println("Skipping ...")
				fmt.Println("!!!!!!!!!!!!!!!!!!!!")
				fmt.Println()

				// Need to decrement since applyTables() will never be called
				atomic.AddInt32(active, -1)
				wg.Done()

				return
			}
		}

		// Request and write file
		fo, err := os.Create(tmpfile)
		checkErr(err)
		defer fo.Close()

		// Chown to mysql user
		os.Chown(tmpfile, downloadInfo.uid, downloadInfo.gid)
		os.Chmod(tmpfile, mysqlPerms)

		// Download files from trite server
		w := bufio.NewWriter(fo)
		ibdresp := getURL(urlfile)
		defer ibdresp.Body.Close()

		sizeServer := ibdresp.ContentLength
		var sizeDown int64
		sizeDown, err = w.ReadFrom(ibdresp.Body) // int of file size returned here
		checkErr(err)
		w.Flush() // Just in case

		// Check if size of file downloaded matches size on server -- Add retry ability
		if sizeDown != sizeServer {
			fmt.Println("\n\nFile download size does not match size on server!")
			fmt.Println(tmpfile, "has been removed.")

			// Remove partial file download
			err = os.Remove(tmpfile)
			checkErr(err)
		}
	}

	// Call applyTables
	applyTables(db, downloadInfo, active, wg)
}

// applyTables performs all of the database actions required to restore a table
func applyTables(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
	filename, _ := parseFileName(downloadInfo.table)
	schemaTrimmed := strings.Trim(downloadInfo.schema, "/")

	// Start db transaction
	tx, err := db.Begin()
	checkErr(err)

	// make the following code work for any settings -- need to preserve before changing so they can be changed back, figure out global vs session and how to handle not setting properly
	_, err = tx.Exec("set session foreign_key_checks=0")
	_, err = tx.Exec("use " + schemaTrimmed)

	switch downloadInfo.engine {
	case "InnoDB":
		// Get table create
		resp := getURL(downloadInfo.taburl + downloadInfo.schema + "tables/" + downloadInfo.table)
		defer resp.Body.Close()
		stmt, _ := ioutil.ReadAll(resp.Body)

		// Drop table if exists
		_, err := tx.Exec("drop table if exists " + addQuotes(filename))
		checkErr(err)

		// Create table
		_, err = tx.Exec(string(stmt))
		checkErr(err)

		// Discard the tablespace
		_, err = tx.Exec("alter table " + addQuotes(filename) + " discard tablespace")
		checkErr(err)

		// Lock the table just in case
		_, err = tx.Exec("lock table " + addQuotes(filename) + " write")
		checkErr(err)

		// Rename happens here
		for _, extension := range downloadInfo.extensions {
			err := os.Rename(downloadInfo.mysqldir+downloadInfo.schema+filename+extension+".trite", downloadInfo.mysqldir+downloadInfo.schema+filename+extension)
			checkErr(err)
		}

		// Import tablespace and analyze otherwise there will be no index statistics
		_, err = tx.Exec("alter table " + addQuotes(filename) + " import tablespace")
		checkErr(err)

		_, err = tx.Exec("analyze local table " + addQuotes(filename))
		checkErr(err)

		// Unlock the table
		_, err = tx.Exec("unlock tables")
		checkErr(err)

		// Commit transaction
		err = tx.Commit()
		checkErr(err)

	case "MyISAM":
		// Drop table if exists
		_, err := tx.Exec("drop table if exists " + addQuotes(filename))
		checkErr(err)

		// Rename happens here
		for _, extension := range downloadInfo.extensions {
			err = os.Rename(downloadInfo.mysqldir+downloadInfo.schema+filename+extension+".trite", downloadInfo.mysqldir+downloadInfo.schema+filename+extension)
			checkErr(err)
		}

		// Commit transaction
		err = tx.Commit()
		checkErr(err)

	default:
		fmt.Println()
		fmt.Println("!!!!!!!!!!!!!!!!!!!!")
		fmt.Println("Backup does not exist or", filename, "is not InnoDB or MyISAM")
		fmt.Println("Skipping ...")
		fmt.Println("!!!!!!!!!!!!!!!!!!!!")
		fmt.Println()
	}

	// Decrement active go routine counter
	fmt.Println(schemaTrimmed + "." + filename + " has been restored")
	atomic.AddInt32(active, -1)
	wg.Done()
}

// applyObjects is a generic function for creating procedures, functions, views and triggers.
func applyObjects(db *sql.DB, objType string, schema string, taburl string) {
	// Start transaction
	tx, err := db.Begin()
	checkErr(err)

	// Use schema
	schemaTrimmed := strings.Trim(schema, "/")
	_, err = tx.Exec("set session foreign_key_checks=0")
	_, err = tx.Exec("use " + schemaTrimmed)

	loc := getURL(taburl + schema + "/" + objType + "s")
	defer loc.Body.Close()
	objects := parseAnchor(loc)
	fmt.Println("Applying", objType+"s for", schemaTrimmed)

	// Only continue if there are objects to create
	if len(objects) > 0 { // ignore when path is empty
		for _, object := range objects {

			filename, _ := parseFileName(object)
			_, err := tx.Exec("drop " + objType + " if exists " + addQuotes(filename))
			resp := getURL(taburl + schema + objType + "s/" + object) // ssssso hacky
			defer resp.Body.Close()
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
