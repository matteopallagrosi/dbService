package utils

type Args struct {
	Key   string
	Value string
}

type Result struct {
	Key   string
	Value string
}

type ServerAddress struct {
	IP   string
	Port string
}

// GetFullAddress restituisce IP e Port separati da ":"
func (s ServerAddress) GetFullAddress() string {
	return s.IP + ":" + s.Port
}
