package gdb

import (
	"fmt"
	"log"
	"sync"

	"github.com/pkg/errors"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	rg "github.com/redislabs/redisgraph-go"
	"golang.org/x/exp/slices"
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
	//g.Delete()
	r.graph = &g
}

func (r *Repo) addCaller(userID string) error {
	log.Println("Creating alerter...")

	// create server if it doesnt already exist
	str := `CREATE (a:Alerter {userID: '%v', keys: %v}) `
	_, err := r.graph.Query(fmt.Sprintf(str, userID, rg.ToString(make([]any, 0))))
	return err
}

func (r *Repo) ChannelSubToAlerter(alerterID, guildID, channelID, key string) error {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `'  }) RETURN a.keys`)

	if err != nil {
		return errors.Wrap(err, "unable to get alerter - is everything defined?")
	}

	if alerterRes.Empty() {
		err = r.addCaller(alerterID)

		if err != nil {
			return errors.Wrap(err, "alerter could not be created")
		}

		alerterRes, err = r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `'  }) RETURN a.keys`)
		if err != nil {
			return errors.Wrap(err, "alerter could not be found even after creating?")
		}

	}

	channelRes, err := r.graph.Query(`MATCH (c:AlerterChannel {guildID: '%v', channelID: '%v' }) RETURN (c)`)
	if err != nil {
		panic(err)
	}
	if channelRes.Empty() {
		// create channel if it doesnt already exist
		str := `CREATE (c:AlerterChannel {guildID: '%v', channelID: '%v' })`
		_, err := r.graph.Query(fmt.Sprintf(str, guildID, channelID))
		if err != nil {
			return errors.Wrap(err, "unable to create AlerterChannel")
		}
	}

	// key stuff
	var keysAnySlice []any
	keysStrs := make([]string, 0)

	for alerterRes.Next() {
		r := alerterRes.Record()
		keysAny := r.GetByIndex(0)
		keysAnySlice, _ = keysAny.([]any)
	}

	for _, v := range keysAnySlice {
		keysStrs = append(keysStrs, v.(string))
	}

	slices.Sort(keysStrs)
	idx, found := slices.BinarySearch(keysStrs, key)

	if !found {
		return errors.New("key not found")
	} else {
		keysStrs = slices.Delete(keysStrs, idx, idx+1)
	}

	anyKeys := []any{}

	for _, v := range keysStrs {
		anyKeys = append(anyKeys, v)
	}
	_, err = r.graph.ParameterizedQuery(`MATCH (a:Alerter {userID: $alerterID}) SET a.keys = $keys`, map[string]any{"alerterID": alerterID, "keys": anyKeys})
	if err != nil {
		return errors.Wrap(err, "unable to create update keys - is everything defined?")
	}

	//

	str := `MATCH (a:Alerter {userID: '` + alerterID + `'}), (ch:AlerterChannel {guildID: '` + guildID + `', channelID: '` + channelID + `' } )  CREATE (ch)-[r:Subscribes]->(a) RETURN r, a, ch`
	res, err := r.graph.Query(str)
	if err != nil {
		return errors.Wrap(err, "unable to create Subscribes relationship - is everything defined?")
	}

	if res.Empty() {
		return errors.New("nothing created when trying to subscribe! Are you sure the alerter and server exist?")
	}

	return nil //r.createAlerterChannel(alerterID, channelID, guildID)
}

func (r *Repo) ServerUnsubToAlerter(alerterID, guildID, channelID string) error {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `'}) RETURN a`)

	if err != nil {
		return err
	}

	if alerterRes.Empty() {
		return errors.New("alerter not found")
	}
	channelRes, err := r.graph.Query(`MATCH (ch:AlerterChannel {guildID: '` + guildID + `', channelID: '` + channelID + `'}) RETURN ch`)
	if err != nil {
		return errors.Wrap(err, "unable to get channel")
	}

	if channelRes.Empty() {
		return errors.New("channel not found")
	}

	_, err = r.graph.Query(`MATCH (ch:ChannelAlerter {guildID: '` + guildID + `', channelID: '` + channelID + `' })-[r:Subscribes]->(a:Alerter {userID: '` + alerterID + `'  })  DELETE r`)
	if err != nil {
		return err
	}

	str := `MATCH (x) WHERE NOT (x)-[]-() DELETE x`
	_, err = r.graph.Query(str)
	if err != nil {
		return errors.Wrap(err, "unable to get prune nodes with 0 relations")
	}

	return nil //r.removeAlerterChannel(alerterID, guildID)
}

func (r *Repo) Generate(alerterID string) (string, error) {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `' })  RETURN a`)

	if err != nil {
		return "", err
	}

	if alerterRes.Empty() { ///TEMPORARY
		err = r.addCaller(alerterID)

		if err != nil {
			return "", errors.Wrap(err, "alerter could not be created")
		}

		alerterRes, err = r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `'  }) RETURN a`)
		if err != nil {
			return "", errors.Wrap(err, "alerter could not be found even after creating?")
		}
	}

	uid := uuid.NewString()
	var keysAnySlice []any
	keysStrs := []string{uid}

	for alerterRes.Next() {
		r := alerterRes.Record()
		keysAny := r.GetByIndex(0)
		keysAnySlice, _ = keysAny.([]any)
	}

	for _, v := range keysAnySlice {
		keysStrs = append(keysStrs, v.(string))
	}

	slices.Sort(keysStrs)

	anyKeys := []any{}

	for _, v := range keysStrs {
		anyKeys = append(anyKeys, v)
	}
	_, err = r.graph.ParameterizedQuery(`MATCH(a:Alerter {userID: $alerterID })SET a.keys = $keys `, map[string]any{"alerterID": alerterID, "keys": anyKeys})
	if err != nil {
		return "", err
	}

	return uid, nil
}

func (r *Repo) ChannelListAllAlerters(channelID string) ([]string, error) {
	alerterRes, err := r.graph.Query(`MATCH (ch:AlerterChannel {channelID: '` + channelID + `' })  RETURN ch`)

	if err != nil {
		return nil, err
	}

	if alerterRes.Empty() {
		return nil, errors.New("channel not found")
	}

	channelRes, err := r.graph.Query(`MATCH (a:Alerter)-[]-(ch:AlerterChannel {channelID: '` + channelID + `' })  RETURN a.userID`)
	if err != nil {
		return nil, err
	}

	res := make([]string, 0)

	for channelRes.Next() {
		rec := channelRes.Record()

		res = append(res, rec.GetByIndex(0).(string))
	}

	return res, nil
}

func (r *Repo) AlerterListAllChannels(alerterID string) (map[string]string, error) {
	alerterRes, err := r.graph.Query(`MATCH (a:Alerter {userID: '` + alerterID + `'  }) RETURN a`)

	if err != nil {
		return nil, err
	}

	if alerterRes.Empty() {
		return nil, errors.New("alerter not found")
	}

	channelRes, err := r.graph.Query(`MATCH (c:AlerterChannel)-[]-(a:Alerter {userID: '` + alerterID + `'})  RETURN c`)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, 0)

	for channelRes.Next() {
		rec := channelRes.Record()

		nodeAny := rec.GetByIndex(0)

		node := nodeAny.(*rg.Node)

		res[node.Properties["guildID"].(string)] = node.Properties["channelID"].(string)
	}
	return res, nil
}
