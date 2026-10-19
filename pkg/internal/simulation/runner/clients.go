package runner

import "fmt"

type clientSelection int

const (
	clientsBoth clientSelection = iota
	clientWgoOnly
	clientKgoOnly
)

func parseClients(value string) (clientSelection, error) {
	switch value {
	case "both":
		return clientsBoth, nil
	case "wgo":
		return clientWgoOnly, nil
	case "kgo":
		return clientKgoOnly, nil
	default:
		return clientsBoth, fmt.Errorf("unknown client %q: choose both, wgo, or kgo", value)
	}
}

func (s clientSelection) includes(client string) bool {
	return s == clientsBoth || s == clientWgoOnly && client == "wgo" || s == clientKgoOnly && client == "kgo"
}

func (s clientSelection) String() string {
	switch s {
	case clientWgoOnly:
		return "wgo"
	case clientKgoOnly:
		return "kgo"
	default:
		return "both"
	}
}
