package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const (
	stamp        = "20060102150405"
	dirPerms     = 0755
	filePerms    = 0644
	sqlExtension = ".sql"
)

// startDump copies creation statements for tables, procedures, functions, triggers and views to a file/directory structure at the path location that trite uses in client mode to restore tables.
func startDump(dir string, dbi *mysqlCredentials) {
	dumpdir := path.Join(dir, dbi.host+"_dump"+time.Now().Format(stamp))
	fmt.Println("Dumping to:", dumpdir)
	fmt.Println()

	// Return a database connection
	db, err := dbi.connect()
	defer db.Close()

	// Problem connecting to database
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Turn off idle connections
	db.SetMaxIdleConns(0)

	// Get a list of schemas in the target database
	db.SetMaxIdleConns(1)
	schemas := schemaList(db)

	// Create dump directory
	err = os.MkdirAll(dumpdir, dirPerms)
	checkErr(err)

	// Schema loop
	count := 0
	total := 0
	fmt.Println()
	for _, schema := range schemas {
		total++
		// Dump schema create
		fmt.Print(schema, ": ")
		dumpSchema(db, dumpdir, schema)

		// Dump table creation statements
		count = dumpTables(db, dumpdir, schema)
		total = total + count
		fmt.Print(count, " tables, ")

		// Dump procedure creation statements
		count = dumpProcs(db, dumpdir, schema)
		total = total + count
		fmt.Print(count, " procedures, ")

		// Dump function creation statements
		count = dumpFuncs(db, dumpdir, schema)
		total = total + count
		fmt.Print(count, " functions, ")

		// Dump trigger creation statements
		count = dumpTriggers(db, dumpdir, schema)
		total = total + count
		fmt.Print(count, " triggers, ")

		// Dump view creation statements
		count = dumpViews(db, dumpdir, schema)
		total = total + count
		fmt.Print(count, " views\n")
	}

	fmt.Println()
	fmt.Println(total, "total objects dumped")
}

// schemaList returns a string slice of schemas to process. MySQL specific schemas like mysql, information_schema and performance_schema are omitted.
func schemaList(db *sql.DB) []string {
	rows, err := db.Query("show databases")
	checkErr(err)

	// Get schema list
	schemas := make([]string, 0)
	var database string
	for rows.Next() {
		err = rows.Scan(&database)
		checkErr(err)

		if database != "mysql" && database != "information_schema" && database != "performance_schema" {
			schemas = append(schemas, database)
		}
	}

	return schemas
}

// dumpSchema creates a file with the schema creation statement.
func dumpSchema(db *sql.DB, dumpdir string, schema string) {
	dir := path.Join(dumpdir, schema)
	var err error

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var ignore string
	var stmt string
	err = db.QueryRow("show create schema "+addQuotes(schema)).Scan(&ignore, &stmt)
	checkErr(err)

	file := path.Join(dir, schema+sqlExtension)
	err = ioutil.WriteFile(file, []byte(stmt+";\n"), filePerms)
	checkErr(err)
}

// dumpTables creates files containing table creation statments. It processes all tables for the schema passed to it. The /tables directory is hardcoded and expected by trite client code.
func dumpTables(db *sql.DB, dumpdir string, schema string) int {
	dir := path.Join(dumpdir, schema, "tables")
	var err error
	count := 0

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var rows *sql.Rows
	rows, err = db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'BASE TABLE'")
	checkErr(err)

	var tableName string
	var ignore string
	var stmt string
	for rows.Next() {
		err = rows.Scan(&tableName)
		checkErr(err)

		err = db.QueryRow("show create table "+addQuotes(schema)+"."+addQuotes(tableName)).Scan(&ignore, &stmt)
		checkErr(err)

		file := path.Join(dir, tableName+sqlExtension)
		err = ioutil.WriteFile(file, []byte(stmt+";\n"), filePerms)
		checkErr(err)

		count++
	}

	return count
}

// dumpProcs creates files containing procedure creation statments. It processes all procedures for the schema passed to it. The /procedures directory is hardcoded and expected by trite client code.
func dumpProcs(db *sql.DB, dumpdir string, schema string) int {
	dir := path.Join(dumpdir, schema, "procedures")
	var err error
	count := 0

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var rows *sql.Rows
	rows, err = db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'PROCEDURE'")
	checkErr(err)

	var procName string
	for rows.Next() {
		err = rows.Scan(&procName)
		checkErr(err)

		var procInfo createInfoStruct
		err = db.QueryRow("show create procedure "+addQuotes(schema)+"."+addQuotes(procName)).Scan(&procInfo.Name, &procInfo.SqlMode, &procInfo.Create, &procInfo.CharsetClient, &procInfo.Collation, &procInfo.DbCollation)
		checkErr(err)

		var jbyte []byte
		jbyte, err = json.MarshalIndent(procInfo, "", "  ")
		checkErr(err)

		file := path.Join(dir, procName+sqlExtension)
		err = ioutil.WriteFile(file, jbyte, filePerms)
		checkErr(err)

		count++
	}

	return count
}

// dumpFuncs creates files containing function creation statments. It processes all functions for the schema passed to it. The /functions directory is hardcoded and expected by trite client code.
func dumpFuncs(db *sql.DB, dumpdir string, schema string) int {
	dir := path.Join(dumpdir, schema, "functions")
	var err error
	count := 0

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var rows *sql.Rows
	rows, err = db.Query("select routine_name from information_schema.routines where routine_schema='" + schema + "' and routine_type = 'FUNCTION'")
	checkErr(err)

	var funcName string
	for rows.Next() {
		err = rows.Scan(&funcName)
		checkErr(err)

		var funcInfo createInfoStruct
		err = db.QueryRow("show create function "+addQuotes(schema)+"."+addQuotes(funcName)).Scan(&funcInfo.Name, &funcInfo.SqlMode, &funcInfo.Create, &funcInfo.CharsetClient, &funcInfo.Collation, &funcInfo.DbCollation)
		checkErr(err)

		var jbyte []byte
		jbyte, err = json.MarshalIndent(funcInfo, "", "  ")
		checkErr(err)

		file := path.Join(dir, funcName+sqlExtension)
		err = ioutil.WriteFile(file, jbyte, filePerms)
		checkErr(err)

		count++
	}

	return count
}

// dumpTriggers creates files containing trigger creation statments. It processes all triggers for the schema passed to it. The /triggers directory is hardcoded and expected by trite client code.
func dumpTriggers(db *sql.DB, dumpdir string, schema string) int {
	dir := path.Join(dumpdir, schema, "triggers")
	var err error
	count := 0

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var rows *sql.Rows
	rows, err = db.Query("select trigger_name from information_schema.triggers where trigger_schema='" + schema + "'")
	checkErr(err)

	var trigName string
	for rows.Next() {
		err = rows.Scan(&trigName)
		checkErr(err)

		var trigInfo createInfoStruct
		err = db.QueryRow("show create trigger "+addQuotes(schema)+"."+addQuotes(trigName)).Scan(&trigInfo.Name, &trigInfo.SqlMode, &trigInfo.Create, &trigInfo.CharsetClient, &trigInfo.Collation, &trigInfo.DbCollation)
		checkErr(err)

		var jbyte []byte
		jbyte, err = json.MarshalIndent(trigInfo, "", "  ")
		checkErr(err)

		file := path.Join(dir, trigName+sqlExtension)
		err = ioutil.WriteFile(file, jbyte, filePerms)
		checkErr(err)

		count++
	}

	return count
}

// dumpViews creates files containing view creation statments. It processes all views for the schema passed to it. The /views directory is hardcoded and expected by trite client code.
func dumpViews(db *sql.DB, dumpdir string, schema string) int {
	dir := path.Join(dumpdir, schema, "views")
	var err error
	count := 0

	err = os.Mkdir(dir, dirPerms)
	checkErr(err)

	var rows *sql.Rows
	rows, err = db.Query("select table_name from information_schema.tables where table_schema='" + schema + "' and table_type = 'VIEW'")
	checkErr(err)

	var view string
	for rows.Next() {
		err = rows.Scan(&view)
		checkErr(err)

		var viewInfo createInfoStruct
		err = db.QueryRow("show create view "+addQuotes(schema)+"."+addQuotes(view)).Scan(&viewInfo.Name, &viewInfo.Create, &viewInfo.CharsetClient, &viewInfo.Collation)
		checkErr(err)

		var jbyte []byte
		jbyte, err = json.MarshalIndent(viewInfo, "", "  ")
		checkErr(err)

		file := path.Join(dir, view+sqlExtension)
		err = ioutil.WriteFile(file, jbyte, filePerms)
		checkErr(err)

		count++
	}

	return count
}
