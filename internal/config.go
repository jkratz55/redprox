package internal

type Config struct {
	ServerPort int      `env:"REDPROX_SERVER_PORT,default=6379"`
	Addrs      []string `env:"REDPROX_ADDRS,required"`
	Username   string   `env:"REDPROX_USERNAME"`
	Password   string   `env:"REDPROX_PASSWORD"`
	CertFile   string   `env:"REDPROX_CERT"`
	KeyFile    string   `env:"REDPROX_KEY"`
	LogLevel   string   `env:"REDPROX_LOG_LEVEL,default=INFO"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	return &cfg, nil
}
