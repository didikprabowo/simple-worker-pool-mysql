package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	maxWorker   int    = 1000
	dbIddleCont int    = 4
	maxJob      int    = 1000000
	core        int    = 4
	urldsn      string = "root:password@/worker"
)

// MySQL
type MySQL struct {
	dbCon *sql.DB
}

var (
	no      int
	errSend *error
	wg      sync.WaitGroup
)

// NewMySQL
func NewMySQL() (MySQL, error) {

	db, err := sql.Open("mysql", urldsn)

	db.SetMaxOpenConns(maxWorker)
	db.SetMaxIdleConns(dbIddleCont)
	db.SetConnMaxLifetime(time.Minute * 5)

	if err != nil {
		return MySQL{dbCon: db}, err
	}

	if err = db.Ping(); err != nil {
		return MySQL{dbCon: db}, err
	}

	return MySQL{dbCon: db}, nil
}

func readData(jobs chan<- int) {
	for i := 0; i <= maxJob; i++ {
		jobs <- i
	}
	close(jobs)
}

// Create Worker and Job
func worker(jobs <-chan int, result chan<- int) {
	for i := 1; i <= maxWorker; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range jobs {
				result <- j
				fmt.Println("Running worker ke", i, " Job ke ", i)
			}
		}(i)
	}
	wg.Wait()
	close(result)

}

// Insert to MySQL of Database
func (m *MySQL) result(result <-chan int) {

	for s := range result {

		defer func() {
			if err := recover(); err != nil {
				*errSend = fmt.Errorf("%v", err)
			}
		}()

		insForm, err := m.dbCon.Prepare("INSERT INTO wo(newid) VALUES(?)")
		if err != nil {
			panic(err.Error())
		}
		_, err = insForm.Exec(s)

		err = insForm.Close()

		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {

	runtime.GOMAXPROCS(core)

	mulai := time.Now()

	jobs := make(chan int)
	results := make(chan int)
	var wg sync.WaitGroup

	go readData(jobs)
	go worker(jobs, results)

	db, err := NewMySQL()

	if err != nil {
		log.Fatal(err)
	}

	db.result(results)

	wg.Wait()

	diff := time.Since(mulai)

	fmt.Println("Waktu selesai : ", diff.Seconds(), "seconds")
}
