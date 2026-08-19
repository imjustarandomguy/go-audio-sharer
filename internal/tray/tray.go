// Package tray provides small cross-platform systray helpers used by both the
// audio server and client apps (KDE Plasma via StatusNotifier, Windows, macOS).
package tray

import (
	_ "embed"
	"runtime"

	"fyne.io/systray"
)

//go:embed icons/server.png
var serverPNG []byte

//go:embed icons/client.png
var clientPNG []byte

//go:embed icons/server.ico
var serverICO []byte

//go:embed icons/client.ico
var clientICO []byte

// ServerIcon returns the icon bytes appropriate for the current platform
// (Windows requires .ico; other platforms use .png).
func ServerIcon() []byte {
	if runtime.GOOS == "windows" {
		return serverICO
	}
	return serverPNG
}

// ClientIcon returns the icon bytes appropriate for the current platform.
func ClientIcon() []byte {
	if runtime.GOOS == "windows" {
		return clientICO
	}
	return clientPNG
}

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
