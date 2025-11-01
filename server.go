package main

import (
	"cmp"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/starfederation/datastar-go/datastar"
	"github.com/valyala/bytebufferpool"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"unshift.local/paybaq/models"
	"unshift.local/paybaq/sql"
	"unshift.local/paybaq/util"
	"unshift.local/paybaq/web"
)

const port = 1337

var hotReloadOnlyOnce sync.Once

const LEDGER_SNAPSHOTS_BUCKET = "LEDGER_SNAPSHOTS"
const USER_LEDGERS_BUCKET = "USER_LEDGERS"
const LEDGERS_STREAM = "LEDGERS"
const VIEW_UPDATE_SUBJECT = "VIEW.update"

//go:embed sql/migrations/main/*.sql
var migrationsFS embed.FS

type DataSystem struct {
	Db            *sql.Database
	LedgerBus     *util.PubSub[string, models.Ledger]
	UserLedgerBus *util.PubSub[string, models.UserLedger]
	ViewStateBus  *util.PubSub[string, models.LedgerSignals]
	SignalsMap    map[string]models.LedgerSignals
}

func (ds *DataSystem) Shutdown() {
	ds.Db.Close()
	ds.LedgerBus.Shutdown()
	ds.UserLedgerBus.Shutdown()
	ds.ViewStateBus.Shutdown()
}

func StartServer() {
	ctx := context.Background()

	migrations, err := sql.MigrationsFromFS(migrationsFS, "sql/migrations/main")
	if err != nil {
		log.Fatal(fmt.Errorf("could not get migration paths: %w", err))
	}

	database, err := sql.NewDatabase(ctx, "./data/database.db", migrations)
	if err != nil {
		log.Fatal(fmt.Errorf("could not create database: %w", err))
	}
	defer database.Close()

	dataSystem := &DataSystem{
		Db:            database,
		LedgerBus:     util.New[string, models.Ledger](10),
		UserLedgerBus: util.New[string, models.UserLedger](10),
		ViewStateBus:  util.New[string, models.LedgerSignals](10),
		SignalsMap:    make(map[string]models.LedgerSignals),
	}
	defer dataSystem.Shutdown()

	r := chi.NewRouter()
	r.Use(
		middleware.Logger,
		middleware.Recoverer,
		ensureUserCookie)

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))

	r.Get("/hotreload", hotReloadHandler)

	r.Get("/", templ.Handler(web.Layout("Paybaq", web.PageHome([]models.Ledger{}))).ServeHTTP)

	r.Get("/ledgers/{id}", func(w http.ResponseWriter, r *http.Request) {
		ledgerId := chi.URLParam(r, "id")

		ledger, err := getLedger(r.Context(), dataSystem.Db, ledgerId)
		if err != nil {
			httpError(w, "could not get ledger", http.StatusInternalServerError, err)
			return
		}

		cookie, err := r.Cookie("paybaq_id")
		if err != nil {
			log.Printf("Unable to store user ledger, could not find paybaq_id cookie: %v", err)
		} else {
			insertUserLedger(r.Context(), dataSystem, cookie.Value, ledgerId)
		}

		signals, err := readLedgerSignals(r)
		if err != nil {
			log.Printf("could not read signals: %v", err)
		}

		updateLedgerView(&ledger, &signals)

		templ.Handler(web.Layout("Paybaq", web.PageLedger(ledger))).ServeHTTP(w, r)
	})

	r.Get("/ledgers/{ledger_id}/stream", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received GET %s", r.URL.String())
		ledgerId := chi.URLParam(r, "ledger_id")

		viewSignals, err := readLedgerSignals(r)
		if err != nil {
			log.Printf("could not read signals: %v", err)
		}

		sse := datastar.NewSSE(w, r)
		sse_id := uuid.New().String()
		sse.PatchSignals([]byte(fmt.Sprintf("{conn_id: '%s'}", sse_id)))

		ledgerChan := dataSystem.LedgerBus.Sub(ledgerId)
		defer dataSystem.LedgerBus.Unsub(ledgerChan)
		viewSignalsChan := dataSystem.ViewStateBus.Sub(sse_id)
		defer dataSystem.ViewStateBus.Unsub(viewSignalsChan)
		for {
			select {
			case <-r.Context().Done():
				log.Printf("GET /ledger/%s/stream received context done", ledgerId)
				return
			case ledger := <-ledgerChan:
				if ledger.Id != ledgerId {
					log.Printf("Received ledger %s, expected %s", ledger.Id, ledgerId)
					continue
				}
				log.Printf("New ledger snapshot recieved for ledger %s, patching ledger", ledger.Id)
				patchLedger(sse, dataSystem.Db, ledgerId, &viewSignals)
			case viewSignals := <-viewSignalsChan:
				if viewSignals.ConnectionId != sse_id {
					continue
				}
				log.Printf("Received view change in ledger/%s/stream: %v", ledgerId, viewSignals)
				patchLedger(sse, dataSystem.Db, ledgerId, &viewSignals)
			}
		}
	})

	// Update ledger view
	r.Patch("/ledgers/{ledger_id}/view", func(w http.ResponseWriter, r *http.Request) {
		ledgerId := chi.URLParam(r, "ledger_id")
		datastarSignals, err := getDatastarSignals(r)
		if err != nil {
			httpError(w, "could not read signals", http.StatusInternalServerError, err)
			return
		}
		var signals models.LedgerSignals
		if err := json.Unmarshal(datastarSignals, &signals); err != nil {
			httpError(w, "could not read signals", http.StatusInternalServerError, err)
			return
		}
		log.Printf("Received signals: %v", signals)
		if signals.ConnectionId == "" {
			log.Printf("No connection id provided for %s, skipping", ledgerId)
			return
		}
		dataSystem.ViewStateBus.Pub(signals, signals.ConnectionId)
	})

	// Create ledger
	r.Post("/ledgers", func(w http.ResponseWriter, r *http.Request) {
		ledger, err := createLedger(r.Context(), dataSystem, r.FormValue("name"))
		if err != nil {
			httpError(w, "could not create ledger", http.StatusInternalServerError, err)
			return
		}
		http.Redirect(w, r, "/ledgers/"+ledger.Id, http.StatusSeeOther)
	})

	// Add person to ledger
	r.Post("/ledgers/{ledger_id}/people", func(w http.ResponseWriter, r *http.Request) {
		ledgerId := chi.URLParam(r, "ledger_id")
		signals := struct {
			NewPersonName string `json:"newPersonName"`
		}{}
		if err := datastar.ReadSignals(r, &signals); err != nil {
			httpError(w, "could not add person to ledger", http.StatusInternalServerError, err)
			return
		}
		_, ledger, err := addPerson(r.Context(), dataSystem, ledgerId, signals.NewPersonName)
		if err != nil {
			httpError(w, "could not add person to ledger", http.StatusInternalServerError, err)
			return
		}
		dataSystem.LedgerBus.Pub(ledger, ledgerId)
		dataSystem.LedgerBus.Pub(ledger, "ledger")
	})

	// Get user ledgers
	r.Get("/ledgers", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("paybaq_id")
		if err != nil {
			log.Printf("No paybaq_id cookie found")
			return
		}

		userLedgerIds, err := getUserLedgerIds(r.Context(), dataSystem, cookie.Value)
		if err != nil {
			httpError(w, "could not get user ledgers", http.StatusInternalServerError, err)
			return
		}

		viewSignals := models.LedgerSignals{
			LedgerViews: make(map[string]models.LedgerView),
		}

		sse := datastar.NewSSE(w, r)
		sse_id := uuid.New().String()
		sse.PatchSignals([]byte(fmt.Sprintf("{conn_id: '%s'}", sse_id)))

		// initial patch
		updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)

		// setup channels to get updates that trigger future patches
		userLedgerChan := dataSystem.UserLedgerBus.Sub(cookie.Value)
		defer dataSystem.UserLedgerBus.Unsub(userLedgerChan)

		ledgerChan := dataSystem.LedgerBus.Sub(userLedgerIds...)
		defer func() {
			if ledgerChan != nil {
				dataSystem.LedgerBus.Unsub(ledgerChan)
			}
		}()

		viewSignalsChan := dataSystem.ViewStateBus.Sub(sse_id)
		defer dataSystem.ViewStateBus.Unsub(viewSignalsChan)

		for {
			select {
			case <-r.Context().Done():
				log.Println("GET /ledgers received context done")
				return
			case signals := <-viewSignalsChan:
				if signals.ConnectionId != sse_id {
					continue
				}
				viewSignals = signals
				log.Printf("Received view change: %v", viewSignals)
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			case userLedger := <-userLedgerChan:
				if userLedger.UserId != cookie.Value {
					continue
				}
				log.Printf("User ledgers list updated, patching user ledgers %s", userLedger)
				// reset the ledger watcher with the new list of ledger ids
				dataSystem.LedgerBus.Unsub(ledgerChan)
				ledgerChan = dataSystem.LedgerBus.Sub(userLedgerIds...)
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			case ledger := <-ledgerChan:
				if !slices.Contains(userLedgerIds, ledger.Id) {
					continue
				}
				log.Printf("User ledger updated, patching user ledgers")
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			}
		}
	})

	// Admin page
	r.Get("/adminotaur", func(w http.ResponseWriter, r *http.Request) {
		templ.Handler(web.Layout("Adminotaur", web.PageAdminotaur([]models.LedgerSummary{}))).ServeHTTP(w, r)
	})

	// All ledgers for admin
	r.Get("/adminotaur/ledgers", func(w http.ResponseWriter, r *http.Request) {
		sse := datastar.NewSSE(w, r)
		patchAllLedgerSummaries(sse, dataSystem)
		ledgerChan := dataSystem.LedgerBus.Sub("ledger")
		defer dataSystem.LedgerBus.Unsub(ledgerChan)
		for {
			select {
			case <-r.Context().Done():
				log.Println("GET /adminotaur/ledgers received context done")
				return
			case ledger := <-ledgerChan:
				log.Printf("New event received in /adminotaur/ledgers for ledger %s, patching ledgers", ledger.Id)
				patchAllLedgerSummaries(sse, dataSystem)
			}
		}
	})

	// Rebuild all ledger snapshots from events
	r.Post("/adminotaur/ledgers/rebuild-snapshots", func(w http.ResponseWriter, r *http.Request) {
		// err := viewsDb.WriteTx(r.Context(), func(conn *sqlite.Conn) error {
		// 	return sqlitex.Execute(conn,
		// 		`
		// 		DELETE FROM ledgers
		// 		`,
		// 		nil)
		// })
		// if err != nil {
		// 	httpError(w, "could not delete rows from ledgers table", http.StatusInternalServerError, err)
		// 	return
		// }
		// ledgerEvents := []models.Event{}
		// err = eventsDb.ReadTx(r.Context(), func(conn *sqlite.Conn) error {
		// 	return sqlitex.Execute(conn,
		// 		`
		// 		SELECT * FROM events
		// 		WHERE entity_type = 'ledger'
		// 		ORDER BY id ASC
		// 		`,
		// 		&sqlitex.ExecOptions{
		// 			ResultFunc: func(stmt *sqlite.Stmt) error {
		// 				ledgerEvents = append(ledgerEvents, models.Event{
		// 					EntityType:    models.EntityType(stmt.ColumnText(1)),
		// 					EntityId:      stmt.ColumnText(2),
		// 					EntityVersion: stmt.ColumnInt64(3),
		// 					EventType:     models.EventType(stmt.ColumnText(4)),
		// 					EventPayload:  json.RawMessage(stmt.ColumnText(5)),
		// 					Metadata:      json.RawMessage(stmt.ColumnText(6)),
		// 				})
		// 				return nil
		// 			},
		// 		})
		// })
		// if err != nil {
		// 	httpError(w, "could not get ledger events from events table", http.StatusInternalServerError, err)
		// 	return
		// }
		// for _, event := range ledgerEvents {
		// 	if event.EventType == "ledger_created" {
		// 		ledger := models.Ledger{
		// 			Id: event.EntityId,
		// 		}
		// 		if err := json.Unmarshal([]byte(event.EventPayload), &ledger); err != nil {
		// 			httpError(w, "could not unmarshal ledger_created event data", http.StatusInternalServerError, err)
		// 			return
		// 		}
		// 		err = HandleLedgerCreated(r.Context(), viewsDb, ledger)
		// 		if err != nil {
		// 			httpError(w, "could not handle ledger created event", http.StatusInternalServerError, err)
		// 			return
		// 		}
		// 	}
		// }
	})

	log.Printf("Listening on http://localhost:%d", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), r); err != nil {
		panic(err)
	}
}

func updateLedgers(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	dataSystem *DataSystem,
	ledgerIds []string,
	viewSignals *models.LedgerSignals) {

	log.Printf("Patching user ledgers, %v", ledgerIds)
	ledgers, err := getLedgers(ctx, dataSystem.Db, ledgerIds)
	if err != nil {
		log.Printf("could not get ledgers: %v", err)
		return
	}
	for i := range ledgers {
		updateLedgerView(&ledgers[i], viewSignals)
	}
	slices.SortFunc(ledgers, func(a models.Ledger, b models.Ledger) int {
		return cmp.Compare(a.Name, b.Name)
	})
	if err := sse.PatchElementTempl(web.UserLedgers(ledgers)); err != nil {
		log.Printf("could not patch user ledgers: %v", err)
	}
}

func updateLedgerView(ledger *models.Ledger, viewSignals *models.LedgerSignals) {
	ledgerView := viewSignals.LedgerViews[ledger.Id]
	if ledgerView == (models.LedgerView{}) {
		ledgerView.Tab = "people"
		ledgerView.Mode = "list"
		if viewSignals.LedgerViews == nil {
			viewSignals.LedgerViews = make(map[string]models.LedgerView)
		}
		viewSignals.LedgerViews[ledger.Id] = ledgerView
	}
	ledger.LedgerView = ledgerView
}

// func isDatastarRequest(r *http.Request) bool {
// 	return r.Header.Get("Datastar-Request") == "true"
// }

func httpError(w http.ResponseWriter, message string, code int, err error) {
	log.Printf("%s: %v", message, err)
	http.Error(w, message, code)
}

func hotReloadHandler(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	hotReloadOnlyOnce.Do(func() {
		// Refresh the client page as soon as connection
		// is established. This will occur only once
		// after the server starts.
		sse.ExecuteScript("window.location.reload()")
	})

	// Freeze the event stream until the connection
	// is lost for any reason. This will force the client
	// to attempt to reconnect after the server reboots.
	<-r.Context().Done()
}

func ensureUserCookie(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("paybaq_id")
		if err == nil && c.Value != "" {
			// cookie already exists
			next.ServeHTTP(w, r)
			return
		}
		id := uuid.New().String()
		http.SetCookie(w, &http.Cookie{
			Name:     "paybaq_id",
			Value:    id,
			Path:     "/",
			MaxAge:   31536000, // 1 year
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   true, // if HTTPS
		})
		next.ServeHTTP(w, r)
	})
}

func patchLedger(sse *datastar.ServerSentEventGenerator, db sql.DbConn, ledgerId string, viewSignals *models.LedgerSignals) error {
	ledger, err := getLedger(sse.Context(), db, ledgerId)
	if err != nil {
		log.Printf("could not get ledger: %s", err)
		return err
	}
	updateLedgerView(&ledger, viewSignals)
	return sse.PatchElementTempl(web.Ledger(ledger))
}

func patchAllLedgerSummaries(sse *datastar.ServerSentEventGenerator, ds *DataSystem) error {
	ledgerSummaries, err := getAllLedgerSummaries(sse.Context(), ds)
	if err != nil {
		log.Printf("could not get ledger summaries: %s", err)
		return err
	}
	return sse.PatchElementTempl(web.LedgersList(ledgerSummaries))
}

// func patchUserLedgers(w http.ResponseWriter, r *http.Request, ds *DataSystem) {
// 	cookie, _ := r.Cookie("paybaq_id")
// 	ledgerIds, err := getUserLedgerIds(r.Context(), ds, cookie.Value)
// 	userLedgers, err := getLedgers(r.Context(), ds.Db, ledgerIds)
// 	if err != nil {
// 		httpError(w, "could not get user ledgers", http.StatusInternalServerError, err)
// 		return
// 	}
// 	sse := datastar.NewSSE(w, r)
// 	sse.PatchElementTempl(web.UserLedgers(userLedgers))
// }

func insertUserLedger(ctx context.Context, ds *DataSystem, userCookieId string, ledgerId string) (bool, error) {
	rowsWritten := 0
	err := ds.Db.Write(ctx, func(conn *sqlite.Conn) error {
		execErr := sqlitex.Execute(conn,
			`
			INSERT OR IGNORE INTO user_ledgers (user_cookie_id, ledger_id)
			VALUES (?, ?)
			`,
			&sqlitex.ExecOptions{
				Args: []any{userCookieId, ledgerId},
			},
		)
		if execErr == nil {
			rowsWritten = conn.Changes()
		}
		return execErr
	})
	if err != nil {
		return false, fmt.Errorf("could not insert user ledger: %w", err)
	}
	if rowsWritten > 0 {
		log.Printf("inserted user ledger %s for %s", ledgerId, userCookieId)
		ds.UserLedgerBus.Pub(models.UserLedger{
			UserId:   userCookieId,
			LedgerId: ledgerId,
		}, userCookieId)
	}
	return rowsWritten > 0, err
}

func getLedger(ctx context.Context, db sql.DbConn, ledgerId string) (models.Ledger, error) {
	var ledger models.Ledger
	err := db.Read(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn,
			`
				SELECT data, created_at, updated_at FROM ledgers
				WHERE id = ?
			`,
			&sqlitex.ExecOptions{
				Args: []any{ledgerId},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					if err := json.Unmarshal([]byte(stmt.ColumnText(0)), &ledger); err != nil {
						return fmt.Errorf("could not unmarshal ledger snapshot: %w", err)
					}
					ledger.Id = ledgerId
					created_at, err := sql.StmtTextToTime(stmt, "created_at")
					if err != nil {
						log.Printf("could not parse created_at: %s", err)
					}
					ledger.CreatedAt = created_at
					updated_at, err := sql.StmtTextToTime(stmt, "updated_at")
					if err != nil {
						log.Printf("could not parse updated_at: %s", err)
					}
					ledger.UpdatedAt = updated_at
					return nil
				},
			},
		)
	})
	return ledger, err
}

func getAllLedgerSummaries(ctx context.Context, ds *DataSystem) ([]models.LedgerSummary, error) {
	var ledgers []models.LedgerSummary
	err := ds.Db.Read(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn,
			`
			SELECT id, data, created_at, updated_at FROM ledgers
			`,
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					var ledger models.LedgerSummary
					if err := json.Unmarshal([]byte(stmt.GetText("data")), &ledger); err != nil {
						return fmt.Errorf("could not unmarshal ledger: %w", err)
					}
					ledger.Id = stmt.GetText("id")
					ledger.CreatedAt = sql.StmtJulianToTime(stmt, "created_at")
					ledger.UpdatedAt = sql.StmtJulianToTime(stmt, "updated_at")
					ledgers = append(ledgers, ledger)
					return nil
				},
			},
		)
	})
	return ledgers, err
}

// func getUserLedgers(ctx context.Context, ds *DataSystem, userCookieId string) ([]models.LedgerSummary, error) {
// 	ledgerIds, err := getUserLedgerIds(ctx, ds, userCookieId)
// 	if err != nil {
// 		return []models.LedgerSummary{}, err
// 	}
// 	return getLedgerSummaries(ctx, ds, ledgerIds)
// }

func getUserLedgerIds(ctx context.Context, ds *DataSystem, userCookieId string) ([]string, error) {
	var ledgerIds []string
	err := ds.Db.ReadTx(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn,
			`
			SELECT ledger_id FROM user_ledgers
			WHERE user_cookie_id = ?
			`,
			&sqlitex.ExecOptions{
				Args: []any{userCookieId},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					ledgerIds = append(ledgerIds, stmt.ColumnText(0))
					return nil
				},
			},
		)
	})
	return ledgerIds, err
}

func getLedgers(ctx context.Context, db *sql.Database, ledgerIds []string) ([]models.Ledger, error) {
	var ledgers []models.Ledger
	err := db.ReadTx(ctx, func(conn *sqlite.Conn) error {
		placeholders := strings.Repeat("?,", len(ledgerIds))
		placeholders = strings.TrimSuffix(placeholders, ",")
		query := fmt.Sprintf(
			`SELECT id, data, created_at, updated_at FROM ledgers WHERE id IN (%s)`,
			placeholders,
		)
		return sqlitex.Execute(conn,
			query,
			&sqlitex.ExecOptions{
				Args: toAny(ledgerIds),
				ResultFunc: func(stmt *sqlite.Stmt) error {
					var ledger models.Ledger
					if err := json.Unmarshal([]byte(stmt.GetText("data")), &ledger); err != nil {
						return err
					}
					ledger.Id = stmt.GetText("id")
					ledger.CreatedAt = sql.StmtJulianToTime(stmt, "created_at")
					ledger.UpdatedAt = sql.StmtJulianToTime(stmt, "updated_at")
					ledgers = append(ledgers, ledger)
					return nil
				},
			},
		)
	})
	return ledgers, err
}

// func getLedgerSummaries(ctx context.Context, ds *DataSystem, ledgerIds []string) ([]models.LedgerSummary, error) {
// 	var ledgers []models.LedgerSummary
// 	for _, ledgerId := range ledgerIds {
// 		kv, err := ds.LedgerSnapshots.Get(ctx, ledgerId)
// 		if err != nil {
// 			continue
// 		}
// 		var ledger models.LedgerSummary
// 		if err := json.Unmarshal(kv.Value(), &ledger); err != nil {
// 			return []models.LedgerSummary{}, err
// 		}
// 		ledgers = append(ledgers, ledger)
// 	}
// 	return ledgers, nil
// }

func addPerson(ctx context.Context, dataSystem *DataSystem, ledgerId string, personName string) (models.Person, models.Ledger, error) {
	var person models.Person
	var ledger models.Ledger
	err := dataSystem.Db.WriteTx(ctx, func(conn *sqlite.Conn) error {
		// Get the current ledger and add a person to it
		ledgerTx, errTx := getLedger(ctx, &sql.DbConnWrapper{Conn: conn}, ledgerId)
		if errTx != nil {
			return errTx
		}
		newPersonId := 1
		for _, person := range ledgerTx.People {
			newPersonId = max(newPersonId, person.Id+1)
		}
		person := models.Person{
			Id:   newPersonId,
			Name: personName,
		}
		personData, errTx := json.Marshal(person)
		if errTx != nil {
			return errTx
		}
		if ledgerTx.People == nil {
			ledgerTx.People = make(map[int]models.Person)
		}
		ledgerTx.People[newPersonId] = person
		ledgerTx.Revision += 1

		if errTx := writeLedger(conn, ledgerTx); errTx != nil {
			return errTx
		}
		ledger = ledgerTx

		event := models.Event{
			SubjectType:     models.SubjectTypeLedger,
			SubjectId:       ledgerId,
			SubjectRevision: ledger.Revision,
			EventType:       models.LedgerPersonAdded,
			EventPayload:    personData,
		}

		// Write an event for the person added
		return writeEvent(conn, event)
	})
	if err != nil {
		return models.Person{}, models.Ledger{}, err
	}
	publishLedger(dataSystem, ledger)
	return person, ledger, nil
}

func createLedger(ctx context.Context, dataSystem *DataSystem, name string) (models.Ledger, error) {
	ledgerId := uuid.NewString()
	ledger := models.Ledger{
		Id:       ledgerId,
		SchemaId: "v1",
		Name:     name,
		Revision: 1,
	}
	err := dataSystem.Db.WriteTx(ctx, func(conn *sqlite.Conn) error {
		ledgerData, errTx := json.Marshal(ledger)
		if errTx != nil {
			return fmt.Errorf("could not marshal ledger: %w", errTx)
		}
		errTx = sqlitex.Execute(conn,
			`
			INSERT INTO ledgers (id, data)
			VALUES (?, ?)
			`,
			&sqlitex.ExecOptions{
				Args: []any{ledger.Id, ledgerData},
			},
		)
		if errTx != nil {
			return fmt.Errorf("could not insert ledger: %w", errTx)
		}
		log.Printf("created ledger '%s'[%s]", name, ledgerId)

		event := models.Event{
			SubjectType:     models.SubjectTypeLedger,
			SubjectId:       ledgerId,
			SubjectRevision: ledger.Revision,
			EventType:       models.LedgerCreated,
			EventPayload:    ledgerData,
		}
		return writeEvent(conn, event)
	})
	if err != nil {
		return models.Ledger{}, err
	}
	publishLedger(dataSystem, ledger)
	return ledger, nil
}

func publishLedger(dataSystem *DataSystem, ledger models.Ledger) {
	log.Printf("publishing ledger %s", ledger.Id)
	dataSystem.LedgerBus.Pub(ledger, ledger.Id)
	dataSystem.LedgerBus.Pub(ledger, "ledger")
}

func writeLedger(conn *sqlite.Conn, ledger models.Ledger) error {
	ledgerData, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("could not marshal ledger: %w", err)
	}
	err = sqlitex.Execute(conn,
		`
		UPDATE ledgers 
		SET data = ?, updated_at = CURRENT_TIMESTAMP 
		WHERE id = ?
		`,
		&sqlitex.ExecOptions{
			Args: []any{ledgerData, ledger.Id},
		},
	)
	if err != nil {
		return fmt.Errorf("could not insert ledger: %w", err)
	}
	return nil
}

func writeEvent(conn *sqlite.Conn, event models.Event) error {
	err := sqlitex.Execute(conn,
		`
		INSERT INTO events (subject_type, subject_id, subject_revision, event_type, event_payload)
		VALUES (?, ?, ?, ?, ?)
		`,
		&sqlitex.ExecOptions{
			Args: []any{
				event.SubjectType,
				event.SubjectId,
				event.SubjectRevision,
				event.EventType,
				event.EventPayload,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("could not insert %s event: %w", event.EventType, err)
	}
	log.Printf("wrote %s event for %s[%s]", event.EventType, event.SubjectType, event.SubjectId)
	return nil
}

// ReadSignals extracts Datastar signals from
// an HTTP request and unmarshals them into the signals target,
// which should be a pointer to a struct.
//
// Expects signals in [URL.Query] for [http.MethodGet] requests.
// Expects JSON-encoded signals in [Request.Body] for other request methods.
func getDatastarSignals(r *http.Request) ([]byte, error) {
	var dsInput []byte

	if r.Method == "GET" {
		dsJSON := r.URL.Query().Get(datastar.DatastarKey)
		if dsJSON == "" {
			return []byte{}, nil
		} else {
			dsInput = []byte(dsJSON)
		}
	} else {
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if _, err := buf.ReadFrom(r.Body); err != nil {
			if err == http.ErrBodyReadAfterClose {
				return []byte{}, fmt.Errorf("body already closed, are you sure you created the SSE ***AFTER*** the ReadSignals? %w", err)
			}
			return []byte{}, fmt.Errorf("failed to read body: %w", err)
		}
		dsInput = buf.Bytes()
	}
	return dsInput, nil
}

func readLedgerSignals(r *http.Request) (models.LedgerSignals, error) {
	datastarSignals, err := getDatastarSignals(r)
	if err != nil {
		return models.LedgerSignals{}, fmt.Errorf("could not read signals: %w", err)
	}
	var signals models.LedgerSignals
	if err := json.Unmarshal(datastarSignals, &signals); err != nil {
		return models.LedgerSignals{}, fmt.Errorf("could not unmarshal signals: %w", err)
	}
	return signals, nil
}

func cloneStrings(src []string) []string {
	return append([]string(nil), src...)
}

func toAny(src []string) []any {
	anys := make([]any, len(src))
	for i, v := range src {
		anys[i] = v
	}
	return anys
}
