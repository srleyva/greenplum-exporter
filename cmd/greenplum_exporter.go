package main

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"os"
)

const (
	downQuery        = `SELECT count(*) FROM gp_segment_configuration WHERE status <> 'u';`
	changeQuery      = `SELECT count(*) FROM gp_segment_configuration WHERE mode = 'c';`
	resyncQuery      = `SELECT count(*) FROM gp_segment_configuration WHERE mode = 'r';`
	segmentTestQuery = `SELECT gp_segment_id, count(*) FROM gp_dist_random('pg_class') GROUP BY 1;`
	streamingQuery   = `SELECT procpid, state FROM pg_stat_replication;`
)

// Exporter collects Postgres metrics. It implements prometheus.Collector.
type Exporter struct {
	host       string
	port       int
	username   string
	password   string
	database   string
	DownSegs   prometheus.Gauge
	ChangeSegs prometheus.Gauge
}

// NewExporter returns a new exporter
func NewExporter(port int, host, username, password, database string) *Exporter {
	downsegs := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "staq", Name: "down_segments", Help: "Shows the number of down segments in our cluster"})
	changesegs := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "staq", Name: "change_segments", Help: "Shows the number of mirror segments in our change tracking indicating down segment"})

	prometheus.MustRegister(downsegs)
	prometheus.MustRegister(changesegs)
	return &Exporter{host, port, username, password, database, downsegs, changesegs}
}

// Run performs all the scrapes every 5 minutes
func (e *Exporter) Run() {
	for {
		e.CheckDown()
		e.CheckChangeTracking()
		time.Sleep(5 * time.Minute)
	}
}

func (e *Exporter) newDB() *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", e.username, e.password, e.host, e.database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		e.DownSegs.Set(1000)
		log.Errorf("Trouble connecting to the DB: %v", err)
	}
	return db
}

// CheckDown will check if any segments are down
func (e *Exporter) CheckDown() {
	db := e.newDB()
	defer db.Close()

	var down float64
	err := db.QueryRow(downQuery).Scan(&down)
	if err != nil {
		e.DownSegs.Set(1000)
		log.Errorf("Err: %s", err)
	} else {

		e.DownSegs.Set(down)
	}
}

// CheckChangeTracking will check if mirrors are tracking changes
func (e *Exporter) CheckChangeTracking() {
	db := e.newDB()
	defer db.Close()

	var change float64
	err := db.QueryRow(changeQuery).Scan(&change)
	if err != nil {
		e.ChangeSegs.Set(1000)
		log.Errorf("Err: %s", err)
	} else {

		e.ChangeSegs.Set(change)
	}
}

// Checks if master or standby
func (e *Exporter) IsMaster() bool {
	db := e.newDB()
	defer db.Close()

	var role string
	host, _ := os.Hostname()
	err := db.QueryRow(fmt.Sprintf("select role from gp_segment_configuration where address='%s';", host)).Scan(&role)
	if err != nil {
		log.Errorf("err: %s", err)
	}
	return role == "p"
}

func main() {
	export := NewExporter(5432, "localhost", "gpadmin", "", "postgres")
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/ismaster", func(w http.ResponseWriter, r *http.Request) {
		if export.IsMaster() {
			w.WriteHeader(200)
			w.Write([]byte("I am master"))
		} else {
			w.WriteHeader(400)
			w.Write([]byte("I am not master"))
		}
	})
	log.Info("Serving metrics on port :8080")
	go export.Run()
	log.Fatal(http.ListenAndServe(":8080", nil))
}
