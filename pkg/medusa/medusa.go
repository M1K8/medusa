package medusa

import (
	"log"
	"sync"

	"github.com/bwmarrin/discordgo"
	"github.com/m1k8/medusa/pkg/gdb"
)

type medusa struct {
	repo *gdb.Repo
	s    *discordgo.Session
}

type medusaImpl interface {
	Send(msg, alerterID string, editMessage func(string, string) string) (map[string]*discordgo.MessageReference, error)
	SendEmbeds(alerterID string, embeds []*discordgo.MessageEmbed, editMessage func(string, []*discordgo.MessageEmbed) []*discordgo.MessageEmbed) (map[string]*discordgo.MessageReference, error)
	SendComplex(alerterID string, msg *discordgo.MessageSend, editMessage func(string, *discordgo.MessageSend) *discordgo.MessageSend) (map[string]*discordgo.MessageReference, error)
}

var _ medusaImpl = &medusa{}

var once sync.Once = sync.Once{}
var singleton *medusa

func GetMedusa(session *discordgo.Session, r *gdb.Repo) *medusa {
	once.Do(func() {
		singleton = &medusa{s: session, repo: r}
	})
	return singleton
}

func (t *medusa) Send(msg, alerterID string, editMessage func(string, string) string) (map[string]*discordgo.MessageReference, error) {
	msgRefs := make(map[string]*discordgo.MessageReference, 0)

	allChnnls, err := t.repo.AlerterListAllChannels(alerterID)
	if err != nil {
		return nil, err
	}

	for k, v := range allChnnls {
		chnnlMsg := msg
		if editMessage != nil {
			chnnlMsg = editMessage(k, msg)
			if chnnlMsg == "" {
				continue
			}
		}
		ref, err := t.s.ChannelMessageSend(v[0], chnnlMsg)

		t.s.ChannelMessageSend(v[0], "<@"+v[1]+">")

		if err != nil {
			log.Println("Failed to send message to " + k + ":" + v[0] + " - " + err.Error())
			continue
		}

		if ref != nil && ref.Reference() != nil {
			msgRefs[k] = ref.Reference()
		}
	}

	return msgRefs, nil
}

func (t *medusa) SendEmbeds(alerterID string, embeds []*discordgo.MessageEmbed, editMessage func(string, []*discordgo.MessageEmbed) []*discordgo.MessageEmbed) (map[string]*discordgo.MessageReference, error) {
	msgRefs := make(map[string]*discordgo.MessageReference, 0)

	allChnnls, err := t.repo.AlerterListAllChannels(alerterID)
	if err != nil {
		return nil, err
	}

	for k, v := range allChnnls {

		chnnlEmbeds := embeds
		if editMessage != nil {
			chnnlEmbeds = editMessage(k, embeds)
			if len(chnnlEmbeds) == 0 {
				continue
			}
		}

		ref, err := t.s.ChannelMessageSendEmbeds(v[0], chnnlEmbeds)
		t.s.ChannelMessageSend(v[0], "<@"+v[1]+">")

		if err != nil {
			log.Println("Failed to send message to " + k + ":" + v[0] + " - " + err.Error())
			continue
		}

		if ref != nil && ref.Reference() != nil {
			msgRefs[k] = ref.Reference()
		}
	}

	return msgRefs, nil
}

func (t *medusa) SendComplex(alerterID string, msg *discordgo.MessageSend, editMessage func(string, *discordgo.MessageSend) *discordgo.MessageSend) (map[string]*discordgo.MessageReference, error) {
	msgRefs := make(map[string]*discordgo.MessageReference, 0)

	allChnnls, err := t.repo.AlerterListAllChannels(alerterID)
	if err != nil {
		return nil, err
	}

	for k, v := range allChnnls {
		chnnlMsg := msg
		if editMessage != nil {
			chnnlMsg = editMessage(k, msg)
			if chnnlMsg == nil {
				continue
			}
		}
		ref, err := t.s.ChannelMessageSendComplex(v[0], chnnlMsg)
		t.s.ChannelMessageSend(v[0], "<@"+v[1]+">")

		if err != nil {
			log.Println("Failed to send message to " + k + ":" + v[0] + " - " + err.Error())
			continue
		}

		if ref != nil && ref.Reference() != nil {
			msgRefs[k] = ref.Reference()
		}
	}

	return msgRefs, nil
}
