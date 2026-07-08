package main

import (
	_ "embed"
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/systray"
	"github.com/hashicorp/yamux"
)

//go:embed icon.png
var iconPNG []byte

//go:embed icon.ico
var iconICO []byte

var relayAddr = "SEM_CONFIGURACAO"
var sharedToken = "SEM_CONFIGURACAO"
var agentID = ""

var connected atomic.Bool
var stopSignal chan struct{}
var stopMu sync.Mutex

func main() {
	if relayAddr == "SEM_CONFIGURACAO" || sharedToken == "SEM_CONFIGURACAO" {
		log.Fatal("Este binário não foi compilado corretamente (faltou -ldflags). Abortando.")
	}

	agentID = generateAgentID()

	systray.Run(onReady, onExit)
}

func onReady() {
	if runtime.GOOS == "windows" {
		systray.SetIcon(iconICO)
	} else {
		systray.SetIcon(iconPNG)
	}

	systray.SetTitle("Proxy SCI")
	systray.SetTooltip("Agente de Proxy Residencial - SCI")

	mStatus := systray.AddMenuItem("Status: iniciando...", "")
	mStatus.Disable()
	systray.AddSeparator()
	mToggle := systray.AddMenuItem("Desconectar", "Pausar o agente")
	mQuit := systray.AddMenuItem("Sair", "Fechar o agente completamente")

	startAgent(mStatus, mToggle)

	go func() {
		for {
			select {
			case <-mToggle.ClickedCh:
				if connected.Load() {
					stopAgent(mStatus, mToggle)
				} else {
					startAgent(mStatus, mToggle)
				}
			case <-mQuit.ClickedCh:
				systray.Quit()
				return
			}
		}
	}()
}

func onExit() {
	stopMu.Lock()
	if stopSignal != nil {
		close(stopSignal)
	}
	stopMu.Unlock()
}

func startAgent(mStatus, mToggle *systray.MenuItem) {
	stopMu.Lock()
	stopSignal = make(chan struct{})
	stop := stopSignal
	stopMu.Unlock()

	mToggle.SetTitle("Desconectar")
	mStatus.SetTitle("Status: conectando...")

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			err := connectAndServe(stop, mStatus)
			connected.Store(false)
			mStatus.SetTitle("Status: desconectado, reconectando...")

			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}

			if err != nil {
				log.Println("Conexão com o relay caiu ou falhou:", err)
			}
		}
	}()
}

func stopAgent(mStatus, mToggle *systray.MenuItem) {
	stopMu.Lock()
	if stopSignal != nil {
		close(stopSignal)
		stopSignal = nil
	}
	stopMu.Unlock()

	connected.Store(false)
	mStatus.SetTitle("Status: desconectado (manual)")
	mToggle.SetTitle("Conectar")
}

func generateAgentID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "desconhecido"
	}
	suffixBytes := make([]byte, 4)
	rand.Read(suffixBytes)
	suffix := hex.EncodeToString(suffixBytes)
	return hostname + "-" + suffix
}

func connectAndServe(stop <-chan struct{}, mStatus *systray.MenuItem) error {
	conn, err := net.Dial("tcp", relayAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	handshake := sharedToken + " " + agentID + "\n"
	if _, err := conn.Write([]byte(handshake)); err != nil {
		return err
	}

	session, err := yamux.Client(conn, nil)
	if err != nil {
		return err
	}
	defer session.Close()

	connected.Store(true)
	mStatus.SetTitle("Status: conectado (" + agentID + ")")

	go func() {
		<-stop
		session.Close()
	}()

	for {
		stream, err := session.Accept()
		if err != nil {
			return err
		}
		go handleStream(stream)
	}
}

func handleStream(stream io.ReadWriteCloser) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	line, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimSpace(line)

	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return
	}
	kind, target := parts[0], parts[1]

	targetConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		stream.Write([]byte("ERR " + err.Error() + "\n"))
		return
	}
	defer targetConn.Close()

	stream.Write([]byte("OK\n"))

	if kind == "HTTP" {
		io.Copy(targetConn, reader)
	}

	pipe(stream, targetConn)
}

func pipe(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }()
	go func() { io.Copy(b, a); done <- struct{}{} }()
	<-done
}