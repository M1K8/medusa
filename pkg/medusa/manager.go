package medusa

// Use redisgraph as a backend

//type Manager interface {
//	LAddAlerter(userID)
//	LGenerate() string               // one time key
//	Subscribe(string, string, string) error // userID, channelid, key
//	Unsubscribe(string) error       // userID
//	ListCurrentSubs() []string      //[]UserID
//
//	ServerSubscribeToUser(guildID, channelID, alerterID string) error // guild, guildChanid,
//	ServerUnubscribeToUser(guildID, alerterID string) error
//}

/*
"CREATE (:Alerter {userID:'$userID', keys:[]})-[:subbedBy]->(:Server {guildID:'$guildID', channelID: '$channelID'})"

"MATCH (a:Alerter)-[:subbedBy]->(s:Server) WHERE s.guildID = '$guildID' RETURN s.channelID"

"MATCH (a:Alerter)-[s:subbedBy]->() DELETE s" // only delete the relationship - this is what we want

*/
