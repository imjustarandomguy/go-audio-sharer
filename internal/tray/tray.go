// Package tray provides small cross-platform systray helpers used by both the
// audio server and client apps (KDE Plasma via StatusNotifier, Windows, macOS).
package tray

import (
	_ "embed"
	"fyne.io/systray"
)

//go:embed icons/server.png
var ServerIcon []byte

//go:embed icons/client.png
var ClientIcon []byte

// Run blocks the calling goroutine (the main goroutine) running the tray until
// the app quits. Menus are built inside onReady.
func Run(onReady func(), onExit func()) {
	systray.Run(onReady, onExit)
}

// Quit signals systray to exit, triggering the onExit callback.
func Quit() {
	systray.Quit()
}

// Refresh forces the native menu to rebuild, useful after dynamic items change.
func Refresh() {
	systray.ResetMenu()
}
