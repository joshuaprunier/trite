package main

// Good amount of the std library and go-sql driver for MySQL interaction plus Google's net html sub repo (critical for handling HTTP server dir/file parsing. Any changes to http.Dir formating will break current code! Is there a better or more efficient way to do client/server?
import (
  "bufio"
  "code.google.com/p/go.net/html"
  "database/sql"
  "encoding/json"
  "flag"
  "fmt"
  _ "github.com/go-sql-driver/mysql"
  "io"
  "io/ioutil"
  "log"
  "net/http"
  _ "net/http/pprof"
  "os"
  "os/signal"
  "os/user"
  "runtime/pprof"
  "strconv"
  "strings"
  "sync"
  "sync/atomic"
  "syscall"
  "time"
  "unsafe"
)

// Constant date timestamp for sufficiently unique directory naming, second precision
const (
  Stamp      = "20060102150405"
  DirPerms   = 0755
  FilePerms  = 0644
  MysqlPerms = 0660
)

// Type definitions
type (
  dbInfoStruct struct {
    user     string
    pass     string
    host     string
    port     string
    sock     string
    mysqldir string
    uid      int
    gid      int
  }

  createInfoStruct struct {
    Name           string
    Sql_mode       string
    Create         string
    Charset_client string
    Collation      string
    Db_collation   string
  }

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
  }
)

// Extremely robust and overworked error catch all. ;-) Errors that bubble to the surface need to be handled elsewhere, this is for debugging or unexpected exceptions mostly
func checkErr(e error) {
  if e != nil {
    log.Panic(e)
  }
}

// Light weight HTTP server implimentation - it accepts a port number and two directory paths, one for db object create definitions and another for an xtrabackup processed with the --export flag
func runServer(tablePath string, backupPath string, port string) {
  // Make sure directory passed in has trailing slash
  if strings.HasSuffix(backupPath, "/") == false {
    backupPath = backupPath + "/"
  }

  // Ensure the backup has been prepared for transporting with --export
  check := dirWalk(backupPath, false)
  if check == false {
    fmt.Println()
    fmt.Println()
    fmt.Println("It appears that --export has not be run on your backups!")
    fmt.Println()
    fmt.Println()
    os.Exit(1)
  }

  fmt.Println()
  fmt.Println("Starting server listening on port", port)
  http.Handle("/tables/", http.StripPrefix("/tables/", http.FileServer(http.Dir(tablePath))))
  http.Handle("/backups/", http.StripPrefix("/backups/", http.FileServer(http.Dir(backupPath))))
  err := http.ListenAndServe(":"+port, nil)
  if err != nil {
    if err.Error() == "listen tcp :"+port+": bind: address already in use" {
      fmt.Println()
      fmt.Println()
      fmt.Println("ERROR: Port", port, "is already in use!")
      fmt.Println()
      fmt.Println()
      os.Exit(1)
    } else {
      checkErr(err)
    }
  }
}

// Walk the backup directory and confirm there are .exp files which is proof --export was run
func dirWalk(dir string, flag bool) bool {
  files, ferr := ioutil.ReadDir(dir)
  checkErr(ferr)
  for _, file := range files {
    // Check if file is a .exp, that means --export has been performed on the backup
    _, ext := parseFileName(file.Name())

    // Handle sub dirs recursive function
    if file.IsDir() {
      flag := dirWalk(dir+file.Name()+"/", flag)
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

// Primary workhorse for table & code dumping - it accepts a dumping destination path and db connection info
func runDump(dir string, dbInfo dbInfoStruct) {
  dumpdir := dir+"/"+dbInfo.host+"_dump" + time.Now().Format(Stamp)
  fmt.Println("Dumping to:", dumpdir)
  fmt.Println()

  // Return a database connection with begin transaction
  db := dbConn(dbInfo)
  defer db.Close()
  db.SetMaxIdleConns(1)

  // Get schema list
  schemas := schemaList(db)

  // Create dump directory
  err := os.Mkdir(dumpdir, DirPerms)
  checkErr(err)

  // Schema loop
  count := 0
  total := 0
  fmt.Println()
  for _, schema := range schemas {
    total++ // for schema dump
    fmt.Print(schema,": ")
    dumpSchema(db, dumpdir, schema)

    count = dumpTables(db, dumpdir, schema)
    total = total+count
    fmt.Print(count," tables, ")

    count = dumpProcs(db, dumpdir, schema)
    total = total+count
    fmt.Print(count," procedures, ")

    count = dumpFuncs(db, dumpdir, schema)
    total = total+count
    fmt.Print(count," functions, ")

    count = dumpTriggers(db, dumpdir, schema)
    total = total+count
    fmt.Print(count," triggers, ")

    count = dumpViews(db, dumpdir, schema)
    total = total+count
    fmt.Print(count," views\n")
  }

  fmt.Println()
  fmt.Println(total, "total objects dumped")
}

// Responsible for retrieving database tables & code from server instance. - it accepts a server url (currently requires http:// and should be recoded to just be ip or name) and db connection info
func runClient(url string, port string, workers uint, dbInfo dbInfoStruct) {

  // Pull some database variables out of struct -- might want to just pass the struct and pull out in child functions as well
  mysqldir := dbInfo.mysqldir
  uid := dbInfo.uid
  gid := dbInfo.gid

  // Make sure mysql datadir is writable
  ferr := ioutil.WriteFile(mysqldir+"/trite_test", []byte("delete\n"), MysqlPerms)
  if ferr != nil {
    fmt.Println()
    fmt.Println("The MySQL data directory is not writable as this user!")
    fmt.Println()
    os.Exit(0)
  } else {
    os.Remove(mysqldir + "/trite_test")
  }

  // Make a database connection
  db := dbConn(dbInfo)
  defer db.Close()
  db.SetMaxIdleConns(1)
  db.Exec("set global innodb_import_table_from_xtrabackup=1;")

  // URL variables
  taburl := "http://" + url + ":" + port + "/tables/"
  backurl := "http://" + url + ":" + port + "/backups/"

  // Verify server urls are accessible
  _, ping1 := http.Head(taburl)
  if ping1 != nil {
    fmt.Println()
    fmt.Println()
    fmt.Println("Problem connecting to", taburl)
    fmt.Println("Check that the server is running, port number is correct or that a firewall is not blocking access")
    os.Exit(0)
  }
  _, ping2 := http.Head(backurl)
  if ping2 != nil {
    fmt.Println("Problem connecting to", backurl)
    fmt.Println("Check that the server is running, port number is correct or that a firewall is not blocking access")
    os.Exit(0)
  }

  // Parse html and get a list of schemas to transport
  base := getUrl(taburl)
  defer base.Body.Close()
  schemas := parseAnchor(base)

  // Loop through all schemas and apply tables
  var active int32 = 0 //limits # of concurrent applyTables()
  wg := new(sync.WaitGroup)
  for _, schema := range schemas {

    // Check if schema exists
    schemaTrimmed := strings.Trim(schema, "/")
    checkSchema(db, schemaTrimmed, taburl+schema+schemaTrimmed+".sql")

    // Parse html and get a list of tables to transport
    tablesDir := getUrl(taburl + schema + "/tables")
    defer tablesDir.Body.Close()
    tables := parseAnchor(tablesDir)

    if len(tables) > 0 { // ignore when path is empty
      for _, table := range tables {

        downloadInfo := downloadInfoStruct{taburl: taburl, backurl: backurl, schema: schema, table: table, mysqldir: mysqldir, uid: uid, gid: gid}

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

  // Loop through all schemas and apply triggers, views, procedures & functions
  for _, schema := range schemas {
    tx, err := db.Begin()
    checkErr(err)
    tx.Exec("set session foreign_key_checks=0;")
    tx.Exec("set session sql_log_bin=0;") // need to check if even logging

    // Check if schema exists
    schemaTrimmed := strings.Trim(schema, "/")
    checkSchema(db, schemaTrimmed, taburl+schema+schemaTrimmed+".sql")
    tx.Exec("use " + schemaTrimmed)

    triggersDir := getUrl(taburl + schema + "/triggers")
    defer triggersDir.Body.Close()
    triggers := parseAnchor(triggersDir)
    fmt.Println("Applying triggers for", schemaTrimmed)
    if len(triggers) > 0 { // ignore when path is empty
      for _, trigger := range triggers {
        applyObjects(tx, trigger, "trigger", schema, taburl)
      }
    }

    viewsDir := getUrl(taburl + schema + "/views")
    defer viewsDir.Body.Close()
    views := parseAnchor(viewsDir)
    fmt.Println("Applying views for", schemaTrimmed)
    if len(views) > 0 { // ignore when path is empty
      for _, view := range views {
        applyObjects(tx, view, "view", schema, taburl)
      }
    }

    proceduresDir := getUrl(taburl + schema + "/procedures")
    defer proceduresDir.Body.Close()
    procedures := parseAnchor(proceduresDir)
    fmt.Println("Applying procedures for", schemaTrimmed)
    if len(procedures) > 0 { // ignore when path is empty
      for _, procedure := range procedures {
        applyObjects(tx, procedure, "procedure", schema, taburl)
      }
    }

    functionsDir := getUrl(taburl + schema + "/functions")
    defer functionsDir.Body.Close()
    functions := parseAnchor(functionsDir)
    fmt.Println("Applying functions for", schemaTrimmed)
    if len(functions) > 0 { // ignore when path is empty
      for _, function := range functions {
        applyObjects(tx, function, "function", schema, taburl)
      }
    }
    // Commit transaction
    cerr := tx.Commit()
    checkErr(cerr)
  }

  // Reset global db variables
  db.Exec("set global innodb_import_table_from_xtrabackup=0;")
}

// Return a db connection pointer, do some detection if we should connect as localhost(client) or tcp(dump). Localhost is to hopefully support protected db mode with skip networking. Utf8 character set hardcoded for all connections. Transaction control is left up to other worker functions.
func dbConn(dbInfo dbInfoStruct) *sql.DB {
  // Trap for SIGINT, may need to trap other signals in the future as well
  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, os.Interrupt)
  go func() {
    for sig := range sigChan {
      fmt.Println()
      fmt.Println(sig, "signal caught!")
    }
  }()

  // If password is blank prompt user - Not perfect as it prints the password typed to the screen
  if dbInfo.pass == "" {
    fmt.Println("Enter password: ")
    pwd, err := ReadPassword(0)
    if err != nil {
      fmt.Println(err)
    }
    dbInfo.pass = string(pwd)
  }

  // Determine tcp or socket connection
  var db *sql.DB
  var err error
  if dbInfo.sock != "" {
    db, err = sql.Open("mysql", dbInfo.user+":"+dbInfo.pass+"@unix("+dbInfo.sock+")/")
    checkErr(err)
  } else if dbInfo.host != "" {
    db, err = sql.Open("mysql", dbInfo.user+":"+dbInfo.pass+"@tcp("+dbInfo.host+":"+dbInfo.port+")/")
    checkErr(err)
  } else {
    fmt.Println("should be no else")
  }

  // Ping database to verify credentials
  perr := db.Ping()
  if perr != nil {
    fmt.Println()
    fmt.Println("Unable to access database! Possible incorrect password.")
    fmt.Println()
    os.Exit(1)
  }

  return db
}

// Return a string slice of schemas to back up when runDump() is called. MySQL specific schemas are omitted.
func schemaList(db *sql.DB) []string {
  rows, err := db.Query("show databases")
  checkErr(err)

  // Get schema list
  schemas := []string{}
  for rows.Next() {
    var database string
    err := rows.Scan(&database)
    checkErr(err)
    if database == "mysql" || database == "information_schema" || database == "performance_schema" {
      // do nothing
    } else {
      schemas = append(schemas, database)
    }
  }
  return schemas
}

// Create a file with the results of a show create schema statement. Called by runDump()
func dumpSchema(db *sql.DB, dumpdir string, schema string) {
  derr := os.Mkdir(dumpdir+"/"+schema, DirPerms)
  checkErr(derr)

  var ignore string
  var stmt string
  err := db.QueryRow("show create schema "+schema).Scan(&ignore, &stmt)
  checkErr(err)

  werr := ioutil.WriteFile(dumpdir+"/"+schema+"/"+schema+".sql", []byte(stmt+";\n"), FilePerms)
  checkErr(werr)
}

// Create files with the results of a show create table statments. Does an entire schema passed to it. Hardcoded /tables subdir.
func dumpTables(db *sql.DB, dumpdir string, schema string) int {
  count := 0
  derr := os.Mkdir(dumpdir+"/"+schema+"/tables", DirPerms)
  checkErr(derr)

  rows, err := db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'BASE TABLE'")
  checkErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var tableName string
    err := rows.Scan(&tableName)
    checkErr(err)

    var ignore string
    var stmt string
    qerr := tx.QueryRow("show create table "+tableName).Scan(&ignore, &stmt)
    checkErr(qerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/tables/"+tableName+".sql", []byte(stmt+";\n"), FilePerms)
    checkErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// Create files with the results of a show create procedure statments. Does an entire schema passed to it. Hardcoded /procedures subdir.
func dumpProcs(db *sql.DB, dumpdir string, schema string) int {
  count := 0
  derr := os.Mkdir(dumpdir+"/"+schema+"/procedures", DirPerms)
  checkErr(derr)

  rows, err := db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'PROCEDURE'")
  checkErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var procName string
    err := rows.Scan(&procName)
    checkErr(err)

    procInfo := new(createInfoStruct)
    qerr := tx.QueryRow("show create procedure "+procName).Scan(&procInfo.Name, &procInfo.Sql_mode, &procInfo.Create, &procInfo.Charset_client, &procInfo.Collation, &procInfo.Db_collation)
    checkErr(qerr)

    jbyte, jerr := json.MarshalIndent(procInfo, "", "  ")
    checkErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/procedures/"+procName+".sql", jbyte, FilePerms)
    checkErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// Create files with the results of a show create function statments. Does an entire schema passed to it. Hardcoded /functions subdir.
func dumpFuncs(db *sql.DB, dumpdir string, schema string) int {
  count := 0
  derr := os.Mkdir(dumpdir+"/"+schema+"/functions", DirPerms)
  checkErr(derr)

  rows, err := db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'FUNCTION'")
  checkErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var funcName string
    err := rows.Scan(&funcName)
    checkErr(err)

    funcInfo := new(createInfoStruct)
    qerr := tx.QueryRow("show create function "+funcName).Scan(&funcInfo.Name, &funcInfo.Sql_mode, &funcInfo.Create, &funcInfo.Charset_client, &funcInfo.Collation, &funcInfo.Db_collation)
    checkErr(qerr)

    jbyte, jerr := json.MarshalIndent(funcInfo, "", "  ")
    checkErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/functions/"+funcName+".sql", jbyte, FilePerms)
    checkErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// Create files with the results of a show create trigger statments. Does an entire schema passed to it. Hardcoded /triggers subdir.
func dumpTriggers(db *sql.DB, dumpdir string, schema string) int {
  count := 0

  derr := os.Mkdir(dumpdir+"/"+schema+"/triggers", DirPerms)
  checkErr(derr)

  rows, err := db.Query("select trigger_name from information_schema.triggers where trigger_schema='" + schema + "'")
  checkErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var trigName string
    err := rows.Scan(&trigName)
    checkErr(err)

    trigInfo := new(createInfoStruct)
    qerr := tx.QueryRow("show create trigger "+trigName).Scan(&trigInfo.Name, &trigInfo.Sql_mode, &trigInfo.Create, &trigInfo.Charset_client, &trigInfo.Collation, &trigInfo.Db_collation)
    checkErr(qerr)

    jbyte, jerr := json.MarshalIndent(trigInfo, "", "  ")
    checkErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/triggers/"+trigName+".sql", jbyte, FilePerms)
    checkErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// Create files with the results of a show create view statments. Does an entire schema passed to it. Hardcoded /views subdir.
func dumpViews(db *sql.DB, dumpdir string, schema string) int {
  count := 0
  derr := os.Mkdir(dumpdir+"/"+schema+"/views", DirPerms)
  checkErr(derr)

  rows, err := db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'VIEW'")
  checkErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var view string
    err := rows.Scan(&view)
    checkErr(err)

    viewInfo := new(createInfoStruct)
    qerr := tx.QueryRow("show create view "+view).Scan(&viewInfo.Name, &viewInfo.Create, &viewInfo.Charset_client, &viewInfo.Collation)
    checkErr(qerr)

    jbyte, jerr := json.MarshalIndent(viewInfo, "", "  ")
    checkErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/views/"+view+".sql", jbyte, FilePerms)
    checkErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// Too simple a task for function?
func getUrl(u string) *http.Response {
  resp, err := http.Get(u)
  checkErr(err)

  return resp
}

// Parses the html response from the server and returns a slice of directories & files. This requires the google net/html sub repo.
func parseAnchor(r *http.Response) []string {
  txt := []string{}
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

// Confirm that a schema exists. Look for a more elegant solution, db ping with schema possibly.
func checkSchema(db *sql.DB, schema string, url string) {
  var exist string
  err := db.QueryRow("select 'Y' from information_schema.schemata where schema_name=?", schema).Scan(&exist)
  if err != nil {
    resp := getUrl(url)
    defer resp.Body.Close()
    stmt, _ := ioutil.ReadAll(resp.Body)
    db.QueryRow(string(stmt))

    fmt.Println()
    fmt.Println("Created schema", schema)
    fmt.Println()
  }
}

// Split a file name into 2 pieces and return strings of the base and 3 digit extension
func parseFileName(text string) (string, string) {
  ext := strings.Split(text, ".")
  ext = ext[cap(ext)-1:]
  ret := ext[0]
  file := strings.TrimSuffix(text, "."+ret)

  return file, ret
}

// Responsible for downloading files from the HTTP server. Tied to applyTable and importTable. InnoDB centric right now. Need to add MyISAM support.
func downloadTable(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
  filename, _ := parseFileName(downloadInfo.table)

  // Ensure backup exists and check the engine type
  // Make separate function to determine engine type
  checkresp1, headerr := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".ibd") // Assume InnoDB first
  checkErr(headerr)
  var engine string
  extensions := []string{}
  if checkresp1.StatusCode == 200 {
    engine = "InnoDB"
    extensions = append(extensions,".ibd")
    extensions = append(extensions,".exp")
  } else {
    checkresp2, headerr := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".MYD") // Check for MyISAM
    checkErr(headerr)

    if checkresp2.StatusCode == 200 {
      engine = "MyISAM"
      extensions = append(extensions,".MYI")
      extensions = append(extensions,".MYD")
      extensions = append(extensions,".frm")
    } else {
      engine = "not handled"
    }
  }

  // Update downloadInfo struct with engine type and extensions array
  downloadInfo.engine = engine
  downloadInfo.extensions = extensions

  // Loop through and download all files from extensions array
  for _,extension := range extensions {
    tmpfile := downloadInfo.mysqldir + filename + extension + ".trite"
    urlfile := downloadInfo.backurl + downloadInfo.schema + filename + extension

    // Request and write file
    fo, err := os.Create(tmpfile)
    checkErr(err)
    defer fo.Close()

    // Chown to mysql user
    os.Chown(tmpfile, downloadInfo.uid, downloadInfo.gid)
    os.Chmod(tmpfile, MysqlPerms)

    // Download files from trite server
    w := bufio.NewWriter(fo)
    ibdresp := getUrl(urlfile)
    defer ibdresp.Body.Close()

    sizeServer := ibdresp.ContentLength
    sizeDown, rerr := w.ReadFrom(ibdresp.Body) // int of file size returned here
    checkErr(rerr)
    w.Flush() // Just in case

    // Check if size of file downloaded matches size on server -- Add retry ability
    if sizeDown != sizeServer {
      fmt.Println("\n\nFile download size does not match size on server!")
      fmt.Println(tmpfile, "has been removed.")

      // Remove partial file download
      rmerr := os.Remove(tmpfile)
      checkErr(rmerr)
    }
  }

  // Call applyTables
  applyTables(db, downloadInfo, active, wg)
}

// This function is called for each table to be copied. It sets session some session level variables then determines the tables engine type. Exported table files are then downloaded from the server and imported to the database. All database actions are performed in a transaction.
func applyTables(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
  filename, _ := parseFileName(downloadInfo.table)
  schemaTrimmed := strings.Trim(downloadInfo.schema, "/")

  // Start db transaction
  tx, dberr := db.Begin()
  checkErr(dberr)

  // make the following code work for any settings -- need to preserve before changing so they can be changed back, figure out global vs session and how to handle not setting properly
  tx.Exec("set session foreign_key_checks=0;")
  tx.Exec("set session sql_log_bin=0;") // need to check if even logging
  tx.Exec("set session wait_timeout=18000;") // 30 mins
  tx.Exec("use " + schemaTrimmed)

  switch downloadInfo.engine {
  case "InnoDB":
    // Get table create
    resp := getUrl(downloadInfo.taburl + downloadInfo.schema + "tables/" + downloadInfo.table)
    defer resp.Body.Close()
    stmt, _ := ioutil.ReadAll(resp.Body)

    // Drop table if exists
    _, execerr := tx.Exec("drop table if exists " + filename)
    checkErr(execerr)

    // Create table
    _, err := tx.Exec(string(stmt))
    checkErr(err)

    // Discard the tablespace
    _, eerr := tx.Exec("alter table " + filename + " discard tablespace")
    checkErr(eerr)

    // Lock the table just in case
    _, lerr := tx.Exec("lock table " + filename + " write")
    checkErr(lerr)

    // rename happens here
    for _,extension := range downloadInfo.extensions {
      mverr := os.Rename(downloadInfo.mysqldir + filename + extension + ".trite",downloadInfo.mysqldir + downloadInfo.schema + filename + extension)
      checkErr(mverr)
    }

    // Import tablespace and analyze otherwise there will be no index statistics
    _, err1 := tx.Exec("alter table " + filename + " import tablespace")
    checkErr(err1)

    _, err2 := tx.Exec("analyze local table " + filename)
    checkErr(err2)

    // Unlock the table
    _, uerr := tx.Exec("unlock tables")
    checkErr(uerr)

    // Commit transaction
    err = tx.Commit()
    checkErr(err)

  case "MyISAM":
    // Drop table if exists
    _, execerr := tx.Exec("drop table if exists " + filename)
    checkErr(execerr)

    // Rename happens here
    for _,extension := range downloadInfo.extensions {
      mverr := os.Rename(downloadInfo.mysqldir + filename + extension + ".trite",downloadInfo.mysqldir + downloadInfo.schema + filename + extension)
      checkErr(mverr)
    }

    // Commit transaction
    err := tx.Commit()
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
  fmt.Println(schemaTrimmed+"."+filename+" has been restored")
  atomic.AddInt32(active, -1)
  wg.Done()
}

// Generic MySQL code applier for stored procedures, functions, views, triggers. Events need to be added, missing any others?
func applyObjects(tx *sql.Tx, object string, objType string, schema string, taburl string) {
  filename, _ := parseFileName(object)
  tx.Exec("drop " + objType + " if exists " + filename)
  resp := getUrl(taburl + schema + objType + "s/" + object) // ssssso hacky
  defer resp.Body.Close()
  stmt, _ := ioutil.ReadAll(resp.Body)

  objInfo := new(createInfoStruct)
  jerr := json.Unmarshal(stmt, &objInfo)
  checkErr(jerr)

  // Set session level variables to recreate stored code properly
  if objInfo.Sql_mode != "" {
    tx.Exec("set session sql_mode = '" + objInfo.Sql_mode + "'")
  }
  if objInfo.Charset_client != "" {
    tx.Exec("set session character_set_client = '" + objInfo.Charset_client + "'")
  }
  if objInfo.Collation != "" {
    tx.Exec("set session collation_connection = '" + objInfo.Collation + "'")
  }
  // Should I be setting this????
  if objInfo.Db_collation != "" {
    tx.Exec("set session collation_database = '" + objInfo.Db_collation + "'")
  }

  // Create object
  _, err := tx.Exec(objInfo.Create)
  checkErr(err)
}

// Borrowed from the crypto/ssh/terminal sub repo to accept a password from stdin without local echo.
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

// Show command usage
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

// Awww yeah ... lets get this party started
func main() {
  start := time.Now()

  // Get working directory
  wd, err := os.Getwd()
  checkErr(err)

  // Get mysql uid & gid
  mysqlUser, err := user.Lookup("mysql")
  checkErr(err)
  uid, _ := strconv.Atoi(mysqlUser.Uid)
  gid, _ := strconv.Atoi(mysqlUser.Gid)
  mysqldir := mysqlUser.HomeDir + "/"

  // Profiling flag
  var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
  flagDatadir := flag.String("datadir", mysqldir, "MySQL data directory")

  // Dump flags
  flagDump := flag.Bool("dump", false, "Run in dump mode")
  flagDumpDir := flag.String("dump_dir", wd, "DUMP: Directory for output")

  // Server flags
  flagServer := flag.Bool("server", false, "Run in server mode")
  flagTablePath := flag.String("dump_path", "", "SERVER: Path to create table files")
  flagBackupPath := flag.String("backup_path", "", "SERVER: Path to database backup files")
  flagPort := flag.String("server_port", "12000", "CLIENT/SERVER: HTTP port number") // also used by client

  // Intercept -help
  flagHelp := flag.Bool("help", false, "Command Usage")

  flag.Parse()

  // Profiling
  if *cpuprofile != "" {
    f, err := os.Create(*cpuprofile)
    if err != nil {
      log.Fatal(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  // Default to localhost if no host or socket provided
  if *flagDbSock == "" && *flagDbHost == "" {
    *flagDbHost = "localhost"
  }

  // If MySQL datadir is supplied overwrite what we get from /etc/passwd
  if *flagDatadir != "" {
    mysqldir = *flagDatadir
  }

  // Populate dbInfo struct
  dbInfo := dbInfoStruct{user: *flagDbUser, pass: *flagDbPass, host: *flagDbHost, port: *flagDbPort, sock: *flagDbSock, mysqldir: mysqldir, uid: uid, gid: gid}

  // Detect what functionality is being requested
  if *flagClient {
    if *flagServerHost == "" || *flagDbUser == "" {
      showUsage()
    } else {
      runClient(*flagServerHost, *flagPort, *flagWorkers, dbInfo)
    }
  } else if *flagDump {
    if *flagDbUser == "" {
      showUsage()
    } else {
      runDump(*flagDumpDir, dbInfo)
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

  fmt.Println()
  fmt.Println("Total runtime =", time.Since(start))
}
