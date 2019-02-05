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
	return &Exporter{host, port, username, password, database, downsegs, changesegs}
}

func (e *Exporter) Run() {
	for {
		time.Sleep(5 * time.Second)
		e.CheckDown()
		e.CheckChangeTracking()
	}
}

func (e *Exporter) newDB() *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", e.username, e.password, e.host, e.database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		e.DownSegs.Set(-100)
		log.Errorf("Trouble connecting to the DB: %v", err)
	}
	return db
}

//func (e *Exporter) RunQuery(query string) {
//db := e.newDB()
//defer db.Close()

//var metric float64
//err := db.QueryRow(query).Scan(&metric)
//if err != nil {
//e.DownSegs.Set(-100)
//log.Errorf("Err: %s", err)
//} else {

//e.DownSegs.Set(down)
//}
//}

func (e *Exporter) CheckDown() {
	db := e.newDB()
	defer db.Close()

	var down float64
	err := db.QueryRow(downQuery).Scan(&down)
	log.Infof("Downsegs: %f", down)
	if err != nil {
		e.DownSegs.Set(-100)
		log.Errorf("Err: %s", err)
	} else {

		e.DownSegs.Set(down)
	}
}

func (e *Exporter) CheckChangeTracking() {
	db := e.newDB()
	defer db.Close()

	var change float64
	err := db.QueryRow(changeQuery).Scan(&change)
	if err != nil {
		e.ChangeSegs.Set(-100)
		log.Errorf("Err: %s", err)
	} else {

		e.ChangeSegs.Set(change)
	}
}

func main() {
	http.Handle("/metrics", promhttp.Handler())
	log.Info("Serving metrics on port :8080")
	export := NewExporter(5432, "localhost", "gpadmin", "", "postgres")
	go export.Run()
	log.Fatal(http.ListenAndServe(":8080", nil))
}
