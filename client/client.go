package client

// Good amount of the std library and go-sql driver for MySQL interaction plus Google's net html sub repo (critical for handling HTTP server dir/file parsing. Any changes to http.Dir formating will break current code! Is there a better or more efficient way to do client/server?
import (
  "bufio"
  "code.google.com/p/go.net/html"
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

  "github.com/joshuaprunier/trite/common"
)

const MysqlPerms = 0660

// Type definitions
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
  }
)

// Responsible for retrieving database tables & code from server instance. - it accepts a server url (currently requires http:// and should be recoded to just be ip or name) and db connection info
func RunClient(url string, port string, workers uint, dbInfo common.DbInfoStruct) {

  // Pull some database variables out of struct -- might want to just pass the struct and pull out in child functions as well
  mysqldir := dbInfo.Mysqldir
  uid := dbInfo.Uid
  gid := dbInfo.Gid

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
  db := common.DbConn(dbInfo)
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
    common.CheckErr(err)
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
    common.CheckErr(cerr)
  }

  // Reset global db variables
  db.Exec("set global innodb_import_table_from_xtrabackup=0;")
}

// Too simple a task for function?
func getUrl(u string) *http.Response {
  resp, err := http.Get(u)
  common.CheckErr(err)

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

// Responsible for downloading files from the HTTP server. Tied to applyTable and importTable. InnoDB centric right now. Need to add MyISAM support.
func downloadTable(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
  filename, _ := common.ParseFileName(downloadInfo.table)

  // Ensure backup exists and check the engine type
  // Make separate function to determine engine type
  checkresp1, headerr := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".ibd") // Assume InnoDB first
  common.CheckErr(headerr)
  var engine string
  extensions := []string{}
  if checkresp1.StatusCode == 200 {
    engine = "InnoDB"
    extensions = append(extensions,".ibd")
    extensions = append(extensions,".exp")
  } else {
    checkresp2, headerr := http.Head(downloadInfo.backurl + downloadInfo.schema + filename + ".MYD") // Check for MyISAM
    common.CheckErr(headerr)

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
    common.CheckErr(err)
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
    common.CheckErr(rerr)
    w.Flush() // Just in case

    // Check if size of file downloaded matches size on server -- Add retry ability
    if sizeDown != sizeServer {
      fmt.Println("\n\nFile download size does not match size on server!")
      fmt.Println(tmpfile, "has been removed.")

      // Remove partial file download
      rmerr := os.Remove(tmpfile)
      common.CheckErr(rmerr)
    }
  }

  // Call applyTables
  applyTables(db, downloadInfo, active, wg)
}

// This function is called for each table to be copied. It sets session some session level variables then determines the tables engine type. Exported table files are then downloaded from the server and imported to the database. All database actions are performed in a transaction.
func applyTables(db *sql.DB, downloadInfo downloadInfoStruct, active *int32, wg *sync.WaitGroup) {
  filename, _ := common.ParseFileName(downloadInfo.table)
  schemaTrimmed := strings.Trim(downloadInfo.schema, "/")

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

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
    common.CheckErr(execerr)

    // Create table
    _, err := tx.Exec(string(stmt))
    common.CheckErr(err)

    // Discard the tablespace
    _, eerr := tx.Exec("alter table " + filename + " discard tablespace")
    common.CheckErr(eerr)

    // Lock the table just in case
    _, lerr := tx.Exec("lock table " + filename + " write")
    common.CheckErr(lerr)

    // rename happens here
    for _,extension := range downloadInfo.extensions {
      mverr := os.Rename(downloadInfo.mysqldir + filename + extension + ".trite",downloadInfo.mysqldir + downloadInfo.schema + filename + extension)
      common.CheckErr(mverr)
    }

    // Import tablespace and analyze otherwise there will be no index statistics
    _, err1 := tx.Exec("alter table " + filename + " import tablespace")
    common.CheckErr(err1)

    _, err2 := tx.Exec("analyze local table " + filename)
    common.CheckErr(err2)

    // Unlock the table
    _, uerr := tx.Exec("unlock tables")
    common.CheckErr(uerr)

    // Commit transaction
    err = tx.Commit()
    common.CheckErr(err)

  case "MyISAM":
    // Drop table if exists
    _, execerr := tx.Exec("drop table if exists " + filename)
    common.CheckErr(execerr)

    // Rename happens here
    for _,extension := range downloadInfo.extensions {
      mverr := os.Rename(downloadInfo.mysqldir + filename + extension + ".trite",downloadInfo.mysqldir + downloadInfo.schema + filename + extension)
      common.CheckErr(mverr)
    }

    // Commit transaction
    err := tx.Commit()
    common.CheckErr(err)

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
  filename, _ := common.ParseFileName(object)
  tx.Exec("drop " + objType + " if exists " + filename)
  resp := getUrl(taburl + schema + objType + "s/" + object) // ssssso hacky
  defer resp.Body.Close()
  stmt, _ := ioutil.ReadAll(resp.Body)

  objInfo := new(common.CreateInfoStruct)
  jerr := json.Unmarshal(stmt, &objInfo)
  common.CheckErr(jerr)

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
  common.CheckErr(err)
}

