//go:build windows

package main

import (
	"log"
	"syscall"
)

const aboveNormalPriorityClass = 0x00008000

// boostProcessPriority raises the client process priority on Windows so heavy
// foreground CPU load does not starve the network receive goroutine feeding
// the audio ring buffer.
func boostProcessPriority() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getCurrentProcess := kernel32.NewProc("GetCurrentProcess")
	setPriorityClass := kernel32.NewProc("SetPriorityClass")

	h, _, _ := getCurrentProcess.Call()
	r1, _, err := setPriorityClass.Call(h, aboveNormalPriorityClass)
	if r1 == 0 {
		log.Printf("Could not raise Windows process priority: %v", err)
	}
}
