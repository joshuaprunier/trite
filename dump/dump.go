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
  stamp      = "20060102150405"
  dirPerms   = 0755
  filePerms  = 0644
)

// CreateInfoStruct stores creation information for procedures, functions, triggers and views
type (
  CreateInfoStruct struct {
    Name          string
    SqlMode       string
    Create        string
    CharsetClient string
    Collation     string
    DbCollation   string
  }
)

// RunDump copies creation statements for tables, procedures, functions, triggers and views to a file/directory structure at the path location that trite uses in client mode to restore tables.
func RunDump(dir string, dbInfo *common.DbInfoStruct) {
  dumpdir := dir+"/"+dbInfo.Host+"_dump" + time.Now().Format(stamp)
  fmt.Println("Dumping to:", dumpdir)
  fmt.Println()

  // Return a database connection
  db, err := common.DbConn(dbInfo)
  defer db.Close()

  // Problem connecting to database
  if err != nil {
    fmt.Println(err)
    os.Exit(1)
  }

  // Get a list of schemas in the target database
  db.SetMaxIdleConns(1)
  schemas := schemaList(db)

  // Create dump directory
  err = os.Mkdir(dumpdir, dirPerms)
  common.CheckErr(err)

  // Schema loop
  count := 0
  total := 0
  fmt.Println()
  for _, schema := range schemas {
    total++ // for schema dump
    fmt.Print(schema,": ")
    dumpSchema(db, dumpdir, schema) // Dump schema create

    count = dumpTables(db, dumpdir, schema) // Dump table creation statements
    total = total+count
    fmt.Print(count," tables, ")

    count = dumpProcs(db, dumpdir, schema) // Dump procedure creation statements
    total = total+count
    fmt.Print(count," procedures, ")

    count = dumpFuncs(db, dumpdir, schema) // Dump function creation statements
    total = total+count
    fmt.Print(count," functions, ")

    count = dumpTriggers(db, dumpdir, schema) // Dump trigger creation statements
    total = total+count
    fmt.Print(count," triggers, ")

    count = dumpViews(db, dumpdir, schema) // Dump view creation statements
    total = total+count
    fmt.Print(count," views\n")
  }

  fmt.Println()
  fmt.Println(total, "total objects dumped")
}

// schemaList returns a string slice of schemas to process. MySQL specific schemas like mysql, information_schema and performance_schema are omitted.
func schemaList(db *sql.DB) []string {
  rows, err := db.Query("show databases")
  common.CheckErr(err)

  // Get schema list
  schemas := []string{}
  var database string
  for rows.Next() {
    err = rows.Scan(&database)
    common.CheckErr(err)

    if database == "mysql" || database == "information_schema" || database == "performance_schema" {
      continue // do nothing
    } else {
      schemas = append(schemas, database)
    }
  }

  return schemas
}

// dumpSchema creates a file with the schema creation statement.
func dumpSchema(db *sql.DB, dumpdir string, schema string) {
  var err error
  err = os.Mkdir(dumpdir+"/"+schema, dirPerms)
  common.CheckErr(err)

  var ignore string
  var stmt string
  err = db.QueryRow("show create schema "+schema).Scan(&ignore, &stmt)
  common.CheckErr(err)

  err = ioutil.WriteFile(dumpdir+"/"+schema+"/"+schema+".sql", []byte(stmt+";\n"), filePerms)
  common.CheckErr(err)
}

// dumpTables creates files containing table creation statments. It processes all tables for the schema passed to it. The /tables directory is hardcoded and expected by trite client code.
func dumpTables(db *sql.DB, dumpdir string, schema string) int {
  var err error
  count := 0
  err = os.Mkdir(dumpdir+"/"+schema+"/tables", dirPerms)
  common.CheckErr(err)

  var rows *sql.Rows
  rows, err = db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'BASE TABLE'")
  common.CheckErr(err)

  // Start db transaction
  var tx *sql.Tx
  tx, err = db.Begin()
  common.CheckErr(err)

  _, err = tx.Exec("use " + schema)
  common.CheckErr(err)

  var tableName string
  var ignore string
  var stmt string
  for rows.Next() {
    err = rows.Scan(&tableName)
    common.CheckErr(err)

    err = tx.QueryRow("show create table "+tableName).Scan(&ignore, &stmt)
    common.CheckErr(err)

    err = ioutil.WriteFile(dumpdir+"/"+schema+"/tables/"+tableName+".sql", []byte(stmt+";\n"), filePerms)
    common.CheckErr(err)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// dumpProcs creates files containing procedure creation statments. It processes all procedures for the schema passed to it. The /procedures directory is hardcoded and expected by trite client code.
func dumpProcs(db *sql.DB, dumpdir string, schema string) int {
  var err error
  count := 0
  err = os.Mkdir(dumpdir+"/"+schema+"/procedures", dirPerms)
  common.CheckErr(err)

  var rows *sql.Rows
  rows, err = db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'PROCEDURE'")
  common.CheckErr(err)

  // Start db transaction
  var tx *sql.Tx
  tx, err = db.Begin()
  common.CheckErr(err)

  _, err = tx.Exec("use " + schema)
  common.CheckErr(err)

  var procName string
  for rows.Next() {
    err = rows.Scan(&procName)
    common.CheckErr(err)

    var procInfo common.CreateInfoStruct
    err = tx.QueryRow("show create procedure "+procName).Scan(&procInfo.Name, &procInfo.SqlMode, &procInfo.Create, &procInfo.CharsetClient, &procInfo.Collation, &procInfo.DbCollation)
    common.CheckErr(err)

    var jbyte []byte
    jbyte, err = json.MarshalIndent(procInfo, "", "  ")
    common.CheckErr(err)

    err = ioutil.WriteFile(dumpdir+"/"+schema+"/procedures/"+procName+".sql", jbyte, filePerms)
    common.CheckErr(err)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// dumpFuncs creates files containing function creation statments. It processes all functions for the schema passed to it. The /functions directory is hardcoded and expected by trite client code.
func dumpFuncs(db *sql.DB, dumpdir string, schema string) int {
  var err error
  count := 0
  err = os.Mkdir(dumpdir+"/"+schema+"/functions", dirPerms)
  common.CheckErr(err)

  var rows *sql.Rows
  rows, err = db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'FUNCTION'")
  common.CheckErr(err)

  // Start db transaction
  var tx *sql.Tx
  tx, err = db.Begin()
  common.CheckErr(err)

  _, err = tx.Exec("use " + schema)
  common.CheckErr(err)

  var funcName string
  for rows.Next() {
    err = rows.Scan(&funcName)
    common.CheckErr(err)

    var funcInfo common.CreateInfoStruct
    err = tx.QueryRow("show create function "+funcName).Scan(&funcInfo.Name, &funcInfo.SqlMode, &funcInfo.Create, &funcInfo.CharsetClient, &funcInfo.Collation, &funcInfo.DbCollation)
    common.CheckErr(err)

    var jbyte []byte
    jbyte, err = json.MarshalIndent(funcInfo, "", "  ")
    common.CheckErr(err)

    err = ioutil.WriteFile(dumpdir+"/"+schema+"/functions/"+funcName+".sql", jbyte, filePerms)
    common.CheckErr(err)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// dumpTriggers creates files containing trigger creation statments. It processes all triggers for the schema passed to it. The /triggers directory is hardcoded and expected by trite client code.
func dumpTriggers(db *sql.DB, dumpdir string, schema string) int {
  var err error
  count := 0

  err = os.Mkdir(dumpdir+"/"+schema+"/triggers", dirPerms)
  common.CheckErr(err)

  var rows *sql.Rows
  rows, err = db.Query("select trigger_name from information_schema.triggers where trigger_schema='" + schema + "'")
  common.CheckErr(err)

  // Start db transaction
  var tx *sql.Tx
  tx, err = db.Begin()
  common.CheckErr(err)

  _, err = tx.Exec("use " + schema)
  common.CheckErr(err)

  var trigName string
  for rows.Next() {
    err = rows.Scan(&trigName)
    common.CheckErr(err)

    var trigInfo common.CreateInfoStruct
    err = tx.QueryRow("show create trigger "+trigName).Scan(&trigInfo.Name, &trigInfo.SqlMode, &trigInfo.Create, &trigInfo.CharsetClient, &trigInfo.Collation, &trigInfo.DbCollation)
    common.CheckErr(err)

    var jbyte []byte
    jbyte, err = json.MarshalIndent(trigInfo, "", "  ")
    common.CheckErr(err)

    err = ioutil.WriteFile(dumpdir+"/"+schema+"/triggers/"+trigName+".sql", jbyte, filePerms)
    common.CheckErr(err)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}

// dumpViews creates files containing view creation statments. It processes all views for the schema passed to it. The /views directory is hardcoded and expected by trite client code.
func dumpViews(db *sql.DB, dumpdir string, schema string) int {
  var err error
  count := 0

  err = os.Mkdir(dumpdir+"/"+schema+"/views", dirPerms)
  common.CheckErr(err)

  var rows *sql.Rows
  rows, err = db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'VIEW'")
  common.CheckErr(err)

  // Start db transaction
  var tx *sql.Tx
  tx, err = db.Begin()
  common.CheckErr(err)

  _, err = tx.Exec("use " + schema)
  common.CheckErr(err)

  var view string
  for rows.Next() {
    err = rows.Scan(&view)
    common.CheckErr(err)

    var viewInfo common.CreateInfoStruct
    err = tx.QueryRow("show create view "+view).Scan(&viewInfo.Name, &viewInfo.Create, &viewInfo.CharsetClient, &viewInfo.Collation)
    common.CheckErr(err)

    var jbyte []byte
    jbyte, err = json.MarshalIndent(viewInfo, "", "  ")
    common.CheckErr(err)

    err = ioutil.WriteFile(dumpdir+"/"+schema+"/views/"+view+".sql", jbyte, filePerms)
    common.CheckErr(err)

    count++
  }

  // Commit transaction
  err = tx.Commit()

  return count
}
