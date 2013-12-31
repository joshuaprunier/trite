package dump

import (
  "database/sql"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "os"
  "time"

  "github.com/joshuaprunier/trite/common"
)

const (
  Stamp      = "20060102150405"
  DirPerms   = 0755
  FilePerms  = 0644
)

type (
  CreateInfoStruct struct {
    Name           string
    Sql_mode       string
    Create         string
    Charset_client string
    Collation      string
    Db_collation   string
  }
)

// Primary workhorse for table & code dumping - it accepts a dumping destination path and db connection info
func RunDump(dir string, dbInfo common.DbInfoStruct) {
  dumpdir := dir+"/"+dbInfo.Host+"_dump" + time.Now().Format(Stamp)
  fmt.Println("Dumping to:", dumpdir)
  fmt.Println()

  // Return a database connection with begin transaction
  db := common.DbConn(dbInfo)
  defer db.Close()
  db.SetMaxIdleConns(1)

  // Get schema list
  schemas := schemaList(db)

  // Create dump directory
  err := os.Mkdir(dumpdir, DirPerms)
  common.CheckErr(err)

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

// Return a string slice of schemas to back up when runDump() is called. MySQL specific schemas are omitted.
func schemaList(db *sql.DB) []string {
  rows, err := db.Query("show databases")
  common.CheckErr(err)

  // Get schema list
  schemas := []string{}
  for rows.Next() {
    var database string
    err := rows.Scan(&database)
    common.CheckErr(err)
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
  common.CheckErr(derr)

  var ignore string
  var stmt string
  err := db.QueryRow("show create schema "+schema).Scan(&ignore, &stmt)
  common.CheckErr(err)

  werr := ioutil.WriteFile(dumpdir+"/"+schema+"/"+schema+".sql", []byte(stmt+";\n"), FilePerms)
  common.CheckErr(werr)
}

// Create files with the results of a show create table statments. Does an entire schema passed to it. Hardcoded /tables subdir.
func dumpTables(db *sql.DB, dumpdir string, schema string) int {
  count := 0
  derr := os.Mkdir(dumpdir+"/"+schema+"/tables", DirPerms)
  common.CheckErr(derr)

  rows, err := db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'BASE TABLE'")
  common.CheckErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var tableName string
    err := rows.Scan(&tableName)
    common.CheckErr(err)

    var ignore string
    var stmt string
    qerr := tx.QueryRow("show create table "+tableName).Scan(&ignore, &stmt)
    common.CheckErr(qerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/tables/"+tableName+".sql", []byte(stmt+";\n"), FilePerms)
    common.CheckErr(werr)

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
  common.CheckErr(derr)

  rows, err := db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'PROCEDURE'")
  common.CheckErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var procName string
    err := rows.Scan(&procName)
    common.CheckErr(err)

    procInfo := new(common.CreateInfoStruct)
    qerr := tx.QueryRow("show create procedure "+procName).Scan(&procInfo.Name, &procInfo.Sql_mode, &procInfo.Create, &procInfo.Charset_client, &procInfo.Collation, &procInfo.Db_collation)
    common.CheckErr(qerr)

    jbyte, jerr := json.MarshalIndent(procInfo, "", "  ")
    common.CheckErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/procedures/"+procName+".sql", jbyte, FilePerms)
    common.CheckErr(werr)

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
  common.CheckErr(derr)

  rows, err := db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'FUNCTION'")
  common.CheckErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var funcName string
    err := rows.Scan(&funcName)
    common.CheckErr(err)

    funcInfo := new(common.CreateInfoStruct)
    qerr := tx.QueryRow("show create function "+funcName).Scan(&funcInfo.Name, &funcInfo.Sql_mode, &funcInfo.Create, &funcInfo.Charset_client, &funcInfo.Collation, &funcInfo.Db_collation)
    common.CheckErr(qerr)

    jbyte, jerr := json.MarshalIndent(funcInfo, "", "  ")
    common.CheckErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/functions/"+funcName+".sql", jbyte, FilePerms)
    common.CheckErr(werr)

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
  common.CheckErr(derr)

  rows, err := db.Query("select trigger_name from information_schema.triggers where trigger_schema='" + schema + "'")
  common.CheckErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var trigName string
    err := rows.Scan(&trigName)
    common.CheckErr(err)

    trigInfo := new(common.CreateInfoStruct)
    qerr := tx.QueryRow("show create trigger "+trigName).Scan(&trigInfo.Name, &trigInfo.Sql_mode, &trigInfo.Create, &trigInfo.Charset_client, &trigInfo.Collation, &trigInfo.Db_collation)
    common.CheckErr(qerr)

    jbyte, jerr := json.MarshalIndent(trigInfo, "", "  ")
    common.CheckErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/triggers/"+trigName+".sql", jbyte, FilePerms)
    common.CheckErr(werr)

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
  common.CheckErr(derr)

  rows, err := db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'VIEW'")
  common.CheckErr(err)

  // Start db transaction
  tx, dberr := db.Begin()
  common.CheckErr(dberr)

  tx.Exec("use " + schema)
  for rows.Next() {
    var view string
    err := rows.Scan(&view)
    common.CheckErr(err)

    viewInfo := new(common.CreateInfoStruct)
    qerr := tx.QueryRow("show create view "+view).Scan(&viewInfo.Name, &viewInfo.Create, &viewInfo.Charset_client, &viewInfo.Collation)
    common.CheckErr(qerr)

    jbyte, jerr := json.MarshalIndent(viewInfo, "", "  ")
    common.CheckErr(jerr)

    werr := ioutil.WriteFile(dumpdir+"/"+schema+"/views/"+view+".sql", jbyte, FilePerms)
    common.CheckErr(werr)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

