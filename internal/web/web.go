// Package web embeds the web player page so it works regardless of the working
// directory the server is launched from.
package web

import _ "embed"

//go:embed index.html
var IndexHTML string
