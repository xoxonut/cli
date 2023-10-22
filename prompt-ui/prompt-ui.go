package promptui

import (
	"github.com/c-bata/go-prompt"
)

func Completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "create", Description: "create <kafka/couchdb> <account_id>"},
		{Text: "read", Description: "read <kafka/couchdb> <account_id>"},
		{Text: "update", Description: "update <kafka/couchdb> <account_id> <target_amount>"},
		{Text: "delete", Description: "delete <kafka/couchdb> <account_id>"},
		{Text: "reset", Description: "reset <kafka/couchdb>"},
		{Text: "send", Description: "send <kafka/couchdb> <giver id> <kafka/couchdb> <receiver id> <amount>"},
		{Text: "kafka", Description: "kafka"},
		{Text: "couchdb", Description: "couchdb"},
		{Text: "exit", Description: "exit"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}
