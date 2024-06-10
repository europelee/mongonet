package mongonet

type SSLPair struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
	Id   string
}

// driver defaults
const (
	DefaultMaxPoolSize                       = 100
	DefaultMaxPoolIdleTimeSec                = 0
	DefaultConnectionPoolHeartbeatIntervalMs = 0
)
