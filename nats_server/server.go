package nats_server

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/a-h/templ"
	"github.com/delaneyj/toolbelt/embeddednats"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/starfederation/datastar-go/datastar"
	"github.com/valyala/bytebufferpool"

	"unshift.local/paybaq/models"
	"unshift.local/paybaq/web"
)

const port = 1337

var hotReloadOnlyOnce sync.Once

var ledgerEvents = make(chan models.Ledger, 10)

const LEDGER_SNAPSHOTS_BUCKET = "LEDGER_SNAPSHOTS"
const USER_LEDGERS_BUCKET = "USER_LEDGERS"
const LEDGERS_STREAM = "LEDGERS"
const VIEW_UPDATE_SUBJECT = "VIEW.update"

type DataSystem struct {
	NatsServer      *embeddednats.Server
	NatsConn        *nats.Conn
	JetStream       jetstream.JetStream
	LedgerStream    jetstream.Stream
	LedgerSnapshots jetstream.KeyValue
	UserLedgers     jetstream.KeyValue
	SignalsMap      map[string]models.LedgerSignals
}

func StartNatsServer() {
	ctx := context.Background()

	// Setup nats server
	ns, err := embeddednats.New(ctx,
		embeddednats.WithDirectory("./data/nats"),
		embeddednats.WithLogging(false, false),
	)
	if err != nil {
		log.Fatal(fmt.Errorf("could not start embedded nats server: %w", err))
	}
	defer ns.Close()
	ns.WaitForServer()

	// setup nats client
	nc, err := ns.Client()
	if err != nil {
		log.Fatal(fmt.Errorf("could not get nats client: %w", err))
	}
	defer nc.Close()

	// setup jetstream ledgers stream and snapshots kv bucket
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(fmt.Errorf("could not create jetstream: %w", err))
	}
	ledgerStream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     LEDGERS_STREAM,
		Subjects: []string{LEDGERS_STREAM + ".>"},
	})
	if err != nil {
		log.Fatal(fmt.Errorf("could not create LEDGERS stream: %w", err))
	}
	snapshotsBucket, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: LEDGER_SNAPSHOTS_BUCKET,
	})
	if err != nil {
		log.Fatal(fmt.Errorf("could not create ledger.snapshots key value store: %w", err))
	}
	userLedgersBucket, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: USER_LEDGERS_BUCKET,
	})
	if err != nil {
		log.Fatal(fmt.Errorf("could not create user.ledgers key value store: %w", err))
	}

	dataSystem := &DataSystem{
		NatsServer:      ns,
		NatsConn:        nc,
		JetStream:       js,
		LedgerStream:    ledgerStream,
		LedgerSnapshots: snapshotsBucket,
		UserLedgers:     userLedgersBucket,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Logger,
		middleware.Recoverer,
		ensureUserCookie)

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))

	r.Get("/hotreload", hotReloadHandler)

	r.Get("/", templ.Handler(web.Layout("Paybaq", web.Home([]models.Ledger{}))).ServeHTTP)

	r.Get("/ledgers/{id}", func(w http.ResponseWriter, r *http.Request) {
		ledgerId := chi.URLParam(r, "id")
		cookie, err := r.Cookie("paybaq_id")
		if err != nil {
			httpError(w, "Could not find paybaq_id cookie", http.StatusNotFound, err)
			return
		}
		insertUserLedger(r.Context(), dataSystem, cookie.Value, ledgerId)
		ledger, err := getLedger(r.Context(), dataSystem, ledgerId)
		if err != nil {
			httpError(w, "could not get ledger", http.StatusInternalServerError, err)
			return
		}
		templ.Handler(web.Layout("Paybaq", web.Ledger(ledger))).ServeHTTP(w, r)
	})

	r.Get("/ledgers/{ledger_id}/stream", func(w http.ResponseWriter, r *http.Request) {
		ledgerId := chi.URLParam(r, "ledger_id")
		var signals models.LedgerSignals
		if err := datastar.ReadSignals(r, &signals); err != nil {
			log.Printf("could not read signals: %v", err)
		}
		log.Printf("Received signals: %v", signals)
		ledgerWatcher, err := dataSystem.LedgerSnapshots.Watch(ctx, ledgerId, jetstream.UpdatesOnly())
		if err != nil {
			httpError(w, "could not set up watcher for ledger", http.StatusInternalServerError, err)
		}
		sse := datastar.NewSSE(w, r)
		sse_id := uuid.New().String()
		sse.PatchSignals([]byte(fmt.Sprintf("{conn_id: '%s'}", sse_id)))
		for {
			select {
			case <-r.Context().Done():
				log.Println("GET /ledger-stream received context done")
				return
			case kv := <-ledgerWatcher.Updates():
				if kv == nil {
					continue
				}
				var ledger models.Ledger
				if err = json.Unmarshal(kv.Value(), &ledger); err != nil {
					log.Printf("could not unmarshal ledger json, %s", kv.Value())
					continue
				}
				if ledger.Id != ledgerId {
					continue
				}
				log.Printf("New ledger snapshot recieved for ledger %s, patching ledger", ledger.Id)
				patchLedger(sse, dataSystem, ledgerId)
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
		if err = dataSystem.NatsConn.Publish(VIEW_UPDATE_SUBJECT, datastarSignals); err != nil {
			log.Printf("could not publish view update for %s, %v", ledgerId, err)
			return
		}
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
		_, err := addPerson(r.Context(), dataSystem, ledgerId, signals.NewPersonName)
		if err != nil {
			httpError(w, "could not add person to ledger", http.StatusInternalServerError, err)
			return
		}
	})

	// Get user ledgers
	r.Get("/ledgers", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("paybaq_id")
		if err != nil {
			log.Printf("No paybaq_id cookie found")
			return
		}
		userLedgerWatcher, err := dataSystem.UserLedgers.Watch(r.Context(), cookie.Value)
		if err != nil {
			httpError(w, "could not watch user ledgers", http.StatusInternalServerError, err)
			return
		}
		userLedgerIds, err := getUserLedgerIds(r.Context(), dataSystem, cookie.Value)
		if err != nil {
			httpError(w, "could not get user ledgers", http.StatusInternalServerError, err)
			return
		}
		ledgerWatcher, _ := dataSystem.LedgerSnapshots.WatchFiltered(r.Context(), cloneStrings(userLedgerIds), jetstream.UpdatesOnly())

		// subscribe to view changes
		viewUpdateChannel := make(chan *nats.Msg)
		viewsSub, err := dataSystem.NatsConn.Subscribe(VIEW_UPDATE_SUBJECT, func(msg *nats.Msg) {
			viewUpdateChannel <- msg
		})
		if err != nil {
			httpError(w, "could not subscribe to view changes", http.StatusInternalServerError, err)
			return
		}
		defer viewsSub.Unsubscribe()

		viewSignals := models.LedgerSignals{
			LedgerViews: make(map[string]models.LedgerView),
		}

		sse_id := uuid.New().String()
		sse := datastar.NewSSE(w, r)
		sse.PatchSignals([]byte(fmt.Sprintf("{conn_id: '%s'}", sse_id)))
		for {
			select {
			case <-r.Context().Done():
				log.Println("GET /ledgers received context done")
				return
			case msg := <-viewUpdateChannel:
				if msg == nil {
					continue
				}
				if err := json.Unmarshal(msg.Data, &viewSignals); err != nil {
					log.Printf("could not unmarshal view signals: %v", err)
					continue
				}
				if viewSignals.ConnectionId != sse_id {
					continue
				}
				log.Printf("Received view change: %v", viewSignals)
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			case update := <-userLedgerWatcher.Updates():
				if update == nil {
					continue
				}
				log.Printf("User ledgers list updated, patching user ledgers %s", update.Value())
				ledgerIdsString := string(update.Value())
				userLedgerIds := strings.Split(ledgerIdsString, ",")
				// reset the ledger watcher with the new list of ledger ids
				ledgerWatcher.Stop()
				ledgerWatcher, _ = dataSystem.LedgerSnapshots.WatchFiltered(r.Context(), cloneStrings(userLedgerIds), jetstream.UpdatesOnly())
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			case kv := <-ledgerWatcher.Updates():
				if kv == nil {
					continue
				}
				log.Printf("User ledger updated, patching user ledgers")
				if kv.Operation() == jetstream.KeyValueDelete {
					updatedLedgerIds := []string{}
					for _, id := range userLedgerIds {
						if id != kv.Key() {
							updatedLedgerIds = append(updatedLedgerIds, id)
						}
					}
					userLedgerIds = updatedLedgerIds
					log.Printf("userLedgerIds set to %v", userLedgerIds)
				}
				updateLedgers(r.Context(), sse, dataSystem, userLedgerIds, &viewSignals)
			}
		}
	})

	// Admin page
	r.Get("/adminotaur", func(w http.ResponseWriter, r *http.Request) {
		templ.Handler(web.Layout("Adminotaur", web.Adminotaur([]models.LedgerSummary{}))).ServeHTTP(w, r)
	})

	// All ledgers for admin
	r.Get("/adminotaur/ledgers", func(w http.ResponseWriter, r *http.Request) {
		sse := datastar.NewSSE(w, r)
		patchAllLedgers(sse, dataSystem)
		for {
			select {
			case <-r.Context().Done():
				log.Println("GET /adminotaur/ledgers received context done")
				return
			case ledger := <-ledgerEvents:
				log.Printf("New event received in /adminotaur/ledgers for ledger %s, patching ledgers", ledger.Id)
				patchAllLedgers(sse, dataSystem)
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
	viewSignals *models.LedgerSignals) error {

	log.Printf("Patching user ledgers, %v", ledgerIds)
	ledgers, err := getLedgers(ctx, dataSystem, ledgerIds)
	if err != nil {
		return fmt.Errorf("could not get ledgers: %v", err)
	}
	for i := range ledgers {
		ledgerView := viewSignals.LedgerViews[ledgers[i].Id]
		if ledgerView == (models.LedgerView{}) {
			ledgerView.Tab = "people"
			ledgerView.Mode = "list"
			viewSignals.LedgerViews[ledgers[i].Id] = ledgerView
		}
		ledgers[i].LedgerView = ledgerView
	}
	slices.SortFunc(ledgers, func(a models.Ledger, b models.Ledger) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return sse.PatchElementTempl(web.UserLedgers(ledgers))
}

func isDatastarRequest(r *http.Request) bool {
	return r.Header.Get("Datastar-Request") == "true"
}

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

func patchLedger(sse *datastar.ServerSentEventGenerator, ds *DataSystem, ledgerId string) error {
	ledger, err := getLedger(sse.Context(), ds, ledgerId)
	if err != nil {
		log.Printf("could not get ledger: %s", err)
		return err
	}
	return sse.PatchElementTempl(web.Ledger(ledger))
}

func patchAllLedgers(sse *datastar.ServerSentEventGenerator, ds *DataSystem) error {
	ledgers, err := getAllLedgers(sse.Context(), ds)
	if err != nil {
		log.Printf("could not get ledgers: %s", err)
		return err
	}
	return sse.PatchElementTempl(web.LedgersList(ledgers))
}

func patchUserLedgers(w http.ResponseWriter, r *http.Request, ds *DataSystem) {
	cookie, _ := r.Cookie("paybaq_id")
	ledgerIds, err := getUserLedgerIds(r.Context(), ds, cookie.Value)
	userLedgers, err := getLedgers(r.Context(), ds, ledgerIds)
	if err != nil {
		httpError(w, "could not get user ledgers", http.StatusInternalServerError, err)
		return
	}
	sse := datastar.NewSSE(w, r)
	sse.PatchElementTempl(web.UserLedgers(userLedgers))
}

func insertUserLedger(ctx context.Context, ds *DataSystem, userCookieId string, ledgerId string) error {
	var ledgerIds []string
	lastVersion := uint64(0)
	kv, err := ds.UserLedgers.Get(ctx, userCookieId)
	if err == nil {
		ledgerIds = strings.Split(string(kv.Value()), ",")
		lastVersion = kv.Revision()
		if slices.Contains(ledgerIds, ledgerId) {
			return nil
		}
		value := []byte(strings.Join(append(ledgerIds, ledgerId), ","))
		_, err = ds.UserLedgers.Update(ctx, userCookieId, value, lastVersion)
		if err != nil {
			log.Printf("Unexpected version when updating user ledgers, trying again")
			return insertUserLedger(ctx, ds, userCookieId, ledgerId)
		}
	} else if err == jetstream.ErrKeyNotFound {
		_, err = ds.UserLedgers.Create(ctx, userCookieId, []byte(ledgerId))
		if err != nil {
			return fmt.Errorf("could not save user ledger: %w", err)
		}
	}
	return nil
}

func getLedger(ctx context.Context, ds *DataSystem, ledgerId string) (models.Ledger, error) {
	kv, err := ds.LedgerSnapshots.Get(ctx, ledgerId)
	if err != nil {
		return models.Ledger{}, fmt.Errorf("could not get ledger snapshot from jetstream: %w", err)
	}
	var ledger models.Ledger
	if err := json.Unmarshal(kv.Value(), &ledger); err != nil {
		return models.Ledger{}, fmt.Errorf("could not unmarshal ledger snapshot: %w", err)
	}
	ledger.Revision = kv.Revision()
	return ledger, nil
}

func getAllLedgers(ctx context.Context, ds *DataSystem) ([]models.LedgerSummary, error) {
	var ledgers []models.LedgerSummary
	keyList, err := ds.LedgerSnapshots.ListKeys(ctx)
	if err != nil {
		return []models.LedgerSummary{}, fmt.Errorf("could not list ledger snapshot keys: %w", err)
	}
	for key := range keyList.Keys() {
		kv, err := ds.LedgerSnapshots.Get(ctx, key)
		if err != nil {
			continue
		}
		var ledger models.LedgerSummary
		if err := json.Unmarshal(kv.Value(), &ledger); err != nil {
			return []models.LedgerSummary{}, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}

func getUserLedgers(ctx context.Context, ds *DataSystem, userCookieId string) ([]models.LedgerSummary, error) {
	ledgerIds, err := getUserLedgerIds(ctx, ds, userCookieId)
	if err != nil {
		return []models.LedgerSummary{}, err
	}
	return getLedgerSummaries(ctx, ds, ledgerIds)
}

func getUserLedgerIds(ctx context.Context, ds *DataSystem, userCookieId string) ([]string, error) {
	kv, err := ds.UserLedgers.Get(ctx, userCookieId)
	if err != nil {
		return []string{}, nil // no ledgers for user yet
	}
	ledgerIds := strings.Split(string(kv.Value()), ",")
	return ledgerIds, nil
}

func getLedgers(ctx context.Context, ds *DataSystem, ledgerIds []string) ([]models.Ledger, error) {
	var ledgers []models.Ledger
	for _, ledgerId := range ledgerIds {
		kv, err := ds.LedgerSnapshots.Get(ctx, ledgerId)
		if err != nil {
			continue
		}
		var ledger models.Ledger
		if err := json.Unmarshal(kv.Value(), &ledger); err != nil {
			return []models.Ledger{}, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}

func getLedgerSummaries(ctx context.Context, ds *DataSystem, ledgerIds []string) ([]models.LedgerSummary, error) {
	var ledgers []models.LedgerSummary
	for _, ledgerId := range ledgerIds {
		kv, err := ds.LedgerSnapshots.Get(ctx, ledgerId)
		if err != nil {
			continue
		}
		var ledger models.LedgerSummary
		if err := json.Unmarshal(kv.Value(), &ledger); err != nil {
			return []models.LedgerSummary{}, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}

func addPerson(ctx context.Context, ds *DataSystem, ledgerId string, personName string) (models.Person, error) {
	ledgerSnapshot, err := getLedger(ctx, ds, ledgerId)
	if err != nil {
		return models.Person{}, err
	}
	newPersonId := 1
	for _, person := range ledgerSnapshot.People {
		newPersonId = max(newPersonId, person.Id+1)
	}
	person := models.Person{
		Id:   newPersonId,
		Name: personName,
	}
	personData, err := json.Marshal(person)
	if err != nil {
		return models.Person{}, fmt.Errorf("could not marshal person: %w", err)
	}
	msg_id := jetstream.WithMsgID(fmt.Sprintf("%s-person-%d", ledgerId, person.Id))
	_, err = ds.JetStream.Publish(ctx, fmt.Sprintf("LEDGERS.%s.person_added", ledgerId), personData, msg_id)
	if err != nil {
		return models.Person{}, fmt.Errorf("could not publish person added event: %w", err)
	}
	if ledgerSnapshot.People == nil {
		ledgerSnapshot.People = make(map[int]models.Person)
	}
	ledgerSnapshot.People[person.Id] = person
	ledgerSnapshot.UpdatedAt = time.Now()
	ledgerData, err := json.Marshal(ledgerSnapshot)
	if err != nil {
		return models.Person{}, fmt.Errorf("could not marshal updated ledger: %w", err)
	}
	_, err = ds.LedgerSnapshots.Update(ctx, ledgerId, ledgerData, ledgerSnapshot.Revision)
	for err != nil {
		// TODO: test for the wrong revision error before retrying
		log.Printf("%v", err)
		ledgerSnapshot, err = getLedger(ctx, ds, ledgerId)
		if err != nil {
			return models.Person{}, err
		}
		ledgerSnapshot.People[person.Id] = person
		ledgerData, err := json.Marshal(ledgerSnapshot)
		if err != nil {
			return models.Person{}, fmt.Errorf("could not marshal updated ledger: %w", err)
		}
		_, err = ds.LedgerSnapshots.Update(ctx, ledgerId, ledgerData, ledgerSnapshot.Revision)
	}
	return person, nil
}

func createLedger(ctx context.Context, ds *DataSystem, name string) (models.Ledger, error) {
	ledger := models.Ledger{
		Id:        uuid.NewString(),
		SchemaId:  "v1",
		Name:      name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	ledgerData, err := json.Marshal(ledger)
	if err != nil {
		return models.Ledger{}, fmt.Errorf("could not marshal ledger: %w", err)
	}
	_, err = ds.JetStream.Publish(ctx, fmt.Sprintf("LEDGERS.%s.created", ledger.Id), ledgerData, jetstream.WithMsgID(ledger.Id))
	if err != nil {
		return models.Ledger{}, fmt.Errorf("could not publish ledger created event: %w", err)
	}
	seq, err := ds.LedgerSnapshots.Put(ctx, ledger.Id, ledgerData)
	if err != nil {
		return models.Ledger{}, fmt.Errorf("could not store ledger snapshot: %w", err)
	}
	log.Printf("Created ledger %s with snapshot revision %d", ledger.Id, seq)
	return ledger, nil
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

func cloneStrings(src []string) []string {
	return append([]string(nil), src...)
}
