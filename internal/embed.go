package internal

import (
	"embed"
)

//go:embed  errors.toml
var InternalFS embed.FS
