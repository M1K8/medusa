package gdb

import (
	"errors"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	rg "github.com/redislabs/redisgraph-go"
)

type Repo struct {
	graph *rg.Graph
	addr  string
	close func() error
}

var (
	r    *Repo
	once = sync.Once{}
)

func GetRepo() *Repo {
	once.Do(func() {
		r = &Repo{addr: "redis:6379"}
		conn, _ := redis.Dial("tcp", r.addr)
		r.close = conn.Close

		r.initDB(conn)
	})

	return r
}

func (r *Repo) initDB(conn redis.Conn) {
	g := rg.GraphNew("servers", conn)
	g.Delete()
	r.graph = &g
}

func (r *Repo) AddCaller(userID string) error {
	newCaller := rg.Node{
		Label: "Alerter",
		Properties: map[string]any{
			"userID": userID,
			"keys":   make(map[string]bool, 0),
		},
	}

	r.graph.AddNode(&newCaller)
	_, err := r.graph.Commit()
	return err
}

func (r *Repo) ServerSubToAlerter(alerterID, guildID, channelID, key string) error {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter) WHERE a.userIO = ` + alerterID + ` RETURN a.keys`)

	if err != nil {
		return err
	}

	if alerterRes.Empty() {
		return errors.New("alerter not found")
	}

	var keys map[string]bool

	for alerterRes.Next() {
		r := alerterRes.Record()
		keysAny, ok := r.Get("keys")
		if !ok {
			return errors.New("keys not defined")
		}

		keys = keysAny.(map[string]bool)
		break

	}

	if _, ok := keys[key]; !ok {
		return errors.New("invalid key")
	} else {
		delete(keys, key)
	}

	serverRes, _ := r.graph.Query(`MATCH (s:Server) WHERE s.guildID = ` + guildID + ` return s`)
	if serverRes.Empty() {
		// create server if it doesnt already exist
		newServer := rg.Node{
			Label: "Server",
			Properties: map[string]any{
				"guildID":    guildID,
				"channelIDs": map[string]string{"alerterID": channelID},
			},
		}

		r.graph.AddNode(&newServer)

		_, err := r.graph.Commit()
		if err != nil {
			return err
		}
	} else {
		// update the channel map
		_, err := r.graph.ParameterizedQuery(`MATCH (s:Server) WHERE s.guildID = $guildID SET s.channelIds +=  { $alerterID : $channelID }`, map[string]any{"guildID": guildID, "alerterID": alerterID, "channelID": channelID})
		if err != nil {
			return err
		}
	}

	// update the alerters key by removing the one just used
	_, err = r.graph.ParameterizedQuery(`MATCH (a:Alerter) WHERE a.userIO = $alerterID SET a.keys = $keys`, map[string]any{"alerterID": alerterID, "keys": keys})
	if err != nil {
		return err
	}

	res, err := r.graph.Query(`MATCH (a:Alerter), (s:Server) WHERE a.userID = ` + alerterID + ` AND  s.guildID = ` + guildID + ` CREATE (s)-[r:Subscribes]->(a) RETURN r`)
	if err != nil {
		return err
	}

	res.PrettyPrint()
	return nil
}

func (r *Repo) ServerUnsubToAlerter(alerterID, guildID string) error {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter) WHERE a.userIO = ` + alerterID + ` RETURN a`)

	if err != nil {
		return err
	}

	if alerterRes.Empty() {
		return errors.New("alerter not found")
	}
	serverRes, err := r.graph.Query(`MATCH (s:Server) WHERE s.guildID = ` + guildID + ` RETURN s`)
	if serverRes.Empty() {
		return errors.New("server not found")
	}

	// update the alerters key by removing the one just used
	_, err = r.graph.Query(`MATCH (s:Server) WHERE s.guildID = ` + guildID + ` SET s.channelIDs += { ` + alerterID + ` : NULL} RETURN s`)
	if err != nil {
		return err
	}

	res, err := r.graph.Query(`MATCH (a:Alerter), (s:Server) WHERE a.userID = ` + alerterID + ` AND  s.guildID = ` + guildID + ` DELETE (s)-[r:Subscribes]->(a) RETURN r`)
	if err != nil {
		return err
	}

	res.PrettyPrint()
	return nil
}

func (r *Repo) Generate(alerterID string) (string, error) {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter) WHERE a.userIO = ` + alerterID + ` RETURN a.keys`)

	if err != nil {
		return "", err
	}

	if alerterRes.Empty() {
		return "", errors.New("alerter not found")
	}

	uid := uuid.NewString()
	_, err = r.graph.ParameterizedQuery(`MATCH (a:Alerter) WHERE a.userID = $alerterID SET a.keys +=  { $key : true }`, map[string]any{"alerterID": alerterID, "key": uid})
	if err != nil {
		return "", err
	}

	return uid, nil
}

func (r *Repo) ServerListAllAlerters(guildID string) ([]string, error) {
	alerterRes, err := r.graph.Query(`MATCH (s:Server) WHERE s.guildID = ` + guildID + ` RETURN s`)

	if err != nil {
		return nil, err
	}

	if alerterRes.Empty() {
		return nil, errors.New("server not found")
	}

	serverRes, err := r.graph.Query(`MATCH (a:Alerter)-[]-(s:Server) WHERE s.guildID = ` + guildID + ` RETURN a.userID`)
	if err != nil {
		return nil, err
	}

	res := make([]string, 0)

	for serverRes.Next() {
		rec := serverRes.Record()

		res = append(res, rec.GetByIndex(0).(string))
	}

	return res, nil

}

func (r *Repo) AlerterListAllServers(alerterID string) (map[string]string, error) {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter) WHERE a.userIO = ` + alerterID + ` RETURN a`)

	if err != nil {
		return nil, err
	}

	if alerterRes.Empty() {
		return nil, errors.New("alerter not found")
	}

	serverRes, err := r.graph.Query(`MATCH (a:Alerter)-[]-(s:Server) WHERE a.userID = ` + alerterID + ` RETURN s`)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, 0)

	for serverRes.Next() {
		rec := serverRes.Record()

		guildID, _ := rec.Get("guildID")
		channelID, _ := rec.Get("channelID")

		res[guildID.(string)] = channelID.(string)
	}

	return res, nil

}
