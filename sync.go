package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/jeanphorn/log4go"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	MMI_FANOUT_BUFFER = 8192 //8k strings
)

type YML struct {
	Sourcedatabase struct {
		DatabaseType string `yaml:"type"`
		Servers      []string
		User         string
		Password     string
		Name         string
		Tables       []string
		Dbs          []*sql.DB
	}
	Targetdatabase struct {
		DatabaseType string `yaml:"type"`
		Servers      []string
		User         string
		Password     string
		Name         string
		Tables       []string
		Dbs          []*sql.DB
		Chans        []chan string
		Chans_resp   []chan string
	}
}

func getConfig() (yml YML) {
	hostname, err := os.Hostname()
	check(err)
	defaultname := "config.yml"
	filename := hostname + "." + defaultname
	if _, err := os.Stat(filename); err == nil {
	} else if _, err := os.Stat(defaultname); err == nil {
		filename = defaultname
	} else {
		check(err)
	}
	ymlFile, err := os.Open(filename)
	check(err)
	defer ymlFile.Close()
	check(err)
	t := YML{}
	byteValue, err := ioutil.ReadAll(ymlFile)
	check(err)
	fmt.Println(string(byteValue))
	err = yaml.Unmarshal(byteValue, &t)
	check(err)
	//d, err := yaml.Marshal(&t)
	//check(err)
	//fmt.Printf("--- t dump:\n%s\n", string(d))
	fmt.Printf("--- get config dump:\n%+v\n", t)
	//fmt.Printf("get config.yml database %s\n", t.Database.Name)
	return t
}

var (
	wg_task         sync.WaitGroup
	wg_channels     sync.WaitGroup
	wg_mmi_channels sync.WaitGroup
	MMI_List        = [...]string{"127.0.0.1"}
	/**
	"acr-dca.acr.mcloud.entsvcs.com",
	"atc-dca.atc.mcloud.entsvcs.com",
	//"bng-dca.bng.mcloud.entsvcs.com",
	//"bsi-dca.bsi.mcloud.entsvcs.com", //decommision august 2020
	"edc-dca.edc.mcloud.entsvcs.com",
	"ida-dca.ida.mcloud.entsvcs.com",
	"jptlj-dca.jptlj.mcloud.entsvcs.com",
	"nlr-dca.nlr.mcloud.entsvcs.com",
	"slr-dca.slr.mcloud.entsvcs.com",
	"sph-dca.sph.mcloud.entsvcs.com",
	"syz-dca.syz.mcloud.entsvcs.com",
	"tul-dca.tul.mcloud.entsvcs.com",
	"wyn-dca.wyn.mcloud.entsvcs.com",
	"atc-cr-l4-monitor.mcloud.entsvcs.com",
	"swa-cr-anl1.mcloud.entsvcs.com" /*at core, master aggregator server*/
)
var err error

func yield() {
	//forces parallel execution by microsleep
	time.Sleep(1 * time.Microsecond)
}

func check(e error) {
	if e != nil {
		log.Error(e)
	}
}

func generateCreateSqlFromSrc(db *sql.DB, src string, tgt string, out chan string) {
	log.Info("generate create table from src:" + src + " to trg:" + tgt)
	rows, err := db.Query("show create table " + src)
	check(err)
	defer rows.Close()
	var s string
	var createSql string
	if rows.Next() {
		err := rows.Scan(&s, &createSql)
		check(err)
	}
	createSql = strings.Replace(createSql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	createSql = strings.Replace(createSql, src, tgt, 1)
	//loadSql := strings.Replace(createSql, tgt, tgt+"_LOAD", 1)
	out <- createSql
	//out <- loadSql
	//out <- "truncate table " + tgt + "_LOAD "
}
func generateSqlFromSrc(db *sql.DB, src string, trg string, out chan string) {
	log.Info("start query src:" + src + "trg:" + trg)
	rows, err := db.Query("select * from " + src)
	defer rows.Close()
	if err != nil {
		fmt.Println("Error running query")
		fmt.Println(err)
		return
	}
	//out := make(chan string, 8129)
	cols, err := rows.Columns()
	readCols := make([]interface{}, len(cols))
	writeCols := make([]sql.NullString, len(cols))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}
	out <- "truncate table " + trg
	replace_s := "INSERT INTO " + trg + "("
	//replace_s := "INSERT INTO " + trg + "_LOAD " + "("
	for i, colX := range cols {
		//vals[i] = new(sql.RawBytes)
		if i > 0 {
			//not the first column
			replace_s = replace_s + ", " + colX
		} else {
			replace_s = replace_s + colX
		}
	}
	replace_s = replace_s + ") VALUES ("

	for rows.Next() {
		err = rows.Scan(readCols...)
		// Now you can check each element of vals for nil-ness,
		// and you can use type introspection and type assertions
		// to fetch the column into a typed variable.
		// *** BUILD REPLACE STATEMENT ***
		replace := replace_s
		//time.Sleep(500 * time.Microsecond)
		for i := 0; i < len(readCols); i++ {
			var thisCol string
			if writeCols[i].Valid {
				thisCol = "'" + writeCols[i].String + "'"
			} else {
				//fmt.Printf("found nill")
				thisCol = " null "
			}
			if i > 0 {
				//not the first column
				replace = replace + "," + thisCol
			} else {
				replace = replace + thisCol
			}
		}
		replace = replace + ")"
		//log.Info("sql generated %d : $s",i,replace)
		out <- replace
	}
	//out <- "truncate table " + trg
	//out <- "INSERT INTO " + trg + " SELECT * FROM " + trg + "_LOAD"
	log.Info("sql generation finished ")
}
func excuteSqlFromChan(db *sql.DB, in chan string) {
	log.Info("start excute sql:")
	Tx, err := db.Begin()
	check(err)
	for sql := range in {
		//fmt.Println("dry run:", sql)
		_, err = Tx.Exec(sql)
		check(err)
	}
	log.Info("insert sql finished")
	Tx.Commit()
	log.Info("sql commited")
}

func init_DB_connections() {
	//prepare db connections
	yml.Sourcedatabase.Dbs = make([]*sql.DB, len(yml.Sourcedatabase.Servers))
	yml.Targetdatabase.Dbs = make([]*sql.DB, len(yml.Targetdatabase.Servers))
	yml.Targetdatabase.Chans = make([]chan string, len(yml.Targetdatabase.Servers))
	yml.Targetdatabase.Chans_resp = make([]chan string, len(yml.Targetdatabase.Servers))
	for i, server := range yml.Sourcedatabase.Servers {
		var constring = yml.Sourcedatabase.User + ":" + yml.Sourcedatabase.Password + "@tcp(" + server + ")/" + yml.Sourcedatabase.Name
		fmt.Printf("found database %s index %d\n", constring, i)
		yml.Sourcedatabase.Dbs[i], err = sql.Open(yml.Sourcedatabase.DatabaseType, constring)
		check(err)
	}
	for i, server := range yml.Targetdatabase.Servers {
		var constring = yml.Targetdatabase.User + ":" + yml.Targetdatabase.Password + "@tcp(" + server + ")/" + yml.Targetdatabase.Name
		fmt.Printf("found database %s index %d\n", constring, i)
		yml.Targetdatabase.Dbs[i], err = sql.Open(yml.Targetdatabase.DatabaseType, constring)
		check(err)
		yml.Targetdatabase.Chans[i] = make(chan string, MMI_FANOUT_BUFFER)
		yml.Targetdatabase.Chans_resp[i] = make(chan string, MMI_FANOUT_BUFFER)
	}
}
func close_DB_connections() {
	for i, _ := range yml.Sourcedatabase.Servers {
		yml.Sourcedatabase.Dbs[i].Close()
	}
	for i, _ := range yml.Targetdatabase.Servers {
		yml.Targetdatabase.Dbs[i].Close()
	}
}

var yml YML

func main() {
	yml = getConfig()
	init_DB_connections()
	middle_channel := make(chan string, MMI_FANOUT_BUFFER)
	var wgCreateSql sync.WaitGroup
	var wg sync.WaitGroup
	wgCreateSql.Add(1)
	go func() {
		for j, table := range yml.Sourcedatabase.Tables {
			generateCreateSqlFromSrc(yml.Sourcedatabase.Dbs[0], table, yml.Targetdatabase.Tables[j], middle_channel)
		}
		wgCreateSql.Done()
	}()
	for i, server := range yml.Sourcedatabase.Servers {
		fmt.Println(server)
		wg.Add(1)
		go func() {
			wgCreateSql.Wait()
			for j, table := range yml.Sourcedatabase.Tables {
				generateSqlFromSrc(yml.Sourcedatabase.Dbs[i], table, yml.Targetdatabase.Tables[j], middle_channel)
			}
			fmt.Println("finished generated sql" + server)
			wg.Done()
		}()
	}
	//fan out
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		for sql := range middle_channel {
			for i, _ := range yml.Targetdatabase.Servers {
				yml.Targetdatabase.Chans[i] <- sql
			}
		}
		fmt.Println("finish fanout")
		wg2.Done()
	}()
	var wg3 sync.WaitGroup
	for i, _ := range yml.Targetdatabase.Servers {
		wg3.Add(1)
		go func() {
			excuteSqlFromChan(yml.Targetdatabase.Dbs[i], yml.Targetdatabase.Chans[i])
			wg3.Done()
		}()
	}
	wg.Wait()
	close(middle_channel)
	fmt.Println("closed middle_channel")
	wg2.Wait()
	for i, _ := range yml.Targetdatabase.Servers {
		close(yml.Targetdatabase.Chans[i])
	}
	log.Info("closed target_channel")
	wg3.Wait()
	close_DB_connections()
	log.Info("closed db")
}
