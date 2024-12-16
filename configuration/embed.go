package configuration

import "embed"

//go:embed checker.toml
var ConfigFS embed.FS
