package db

import (
	"database/sql"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/sports/proto/sports"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
)

type SportsRepo interface {
	Init() error

	List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

func (r *sportsRepo) Init() error {
	var err error

	r.init.Do(func() {
		err = r.seed()
	})

	return err
}
func (r *sportsRepo) List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventsQueries()[eventsList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

func (r *sportsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
		orderBy string
	)

	if filter == nil {
		return query, args
	}

	if filter.Visible {
		clauses = append(clauses, "visible=?")
		args = append(args, filter.Visible)
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	if filter.OrderBy != "" {
		orderBy = filter.OrderBy
		query += " ORDER BY advertised_start_time " + orderBy
	}

	return query, args
}

func (m *sportsRepo) scanEvents(rows *sql.Rows) ([]*sports.Event, error) {
	var events []*sports.Event

	for rows.Next() {
		var event sports.Event
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.Name, &advertisedStart, &event.Visible); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}
		if advertisedStart.Before(time.Now()) {
			event.Status = "CLOSED"
		} else {
			event.Status = "OPEN"
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		event.AdvertisedStartTime = ts

		events = append(events, &event)
	}

	return events, nil
}
