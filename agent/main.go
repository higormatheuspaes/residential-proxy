package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/yamux"
)

var relayAddr = "SEM_CONFIGURACAO"
var sharedToken = "SEM_CONFIGURACAO"
var agentID = ""

func main() {
	if relayAddr == "SEM_CONFIGURACAO" || sharedToken == "SEM_CONFIGURACAO" {
		log.Fatal("Este binário não foi compilado corretamente (faltou -ldflags). Abortando.")
	}

	agentID = generateAgentID()
	log.Println("Este agente vai se identificar como:", agentID)

	for {
		err := connectAndServe()
		if err != nil {
			log.Println("Conexão com o relay caiu ou falhou:", err)
		}
		log.Println("Tentando reconectar em 5 segundos...")
		time.Sleep(5 * time.Second)
	}
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

func connectAndServe() error {
	conn, err := net.Dial("tcp", relayAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	handshake := sharedToken + " " + agentID + "\n"
	_, err = conn.Write([]byte(handshake))
	if err != nil {
		return err
	}

	log.Println("Conectado ao relay como", agentID)

	session, err := yamux.Client(conn, nil)
	if err != nil {
		return err
	}

	for {
		stream, err := session.Accept()
		if err != nil {
			return err
		}
		go handleStream(stream)
	}
}

// handleStream lê a instrução do relay, tenta conectar no destino real,
// e AVISA o relay se deu certo (OK) ou errado (ERR <motivo>) antes de
// começar a repassar tráfego. Isso evita que o relay libere o cliente
// (robô/curl) antes de ter certeza que o caminho está realmente aberto.
func handleStream(stream io.ReadWriteCloser) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Erro ao ler instrução do relay:", err)
		return
	}
	line = strings.TrimSpace(line)

	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		log.Println("Instrução malformada do relay:", line)
		return
	}

	kind, target := parts[0], parts[1]

	// Timeout de conexão -- se o destino não responder em 10s, desiste
	// e avisa o relay, em vez de deixar o robô esperando pra sempre.
	targetConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		log.Println("Erro ao conectar no destino", target, ":", err)
		stream.Write([]byte("ERR " + err.Error() + "\n"))
		return
	}
	defer targetConn.Close()

	log.Println("Repassando tráfego para", target, "(tipo:", kind+")")

	// Avisa o relay que a conexão real foi estabelecida com sucesso.
	stream.Write([]byte("OK\n"))

	if kind == "HTTP" {
		io.Copy(targetConn, reader)
	}

	pipe(stream, targetConn)
}

func pipe(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)

	go func() {
		io.Copy(a, b)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(b, a)
		done <- struct{}{}
	}()

	<-done
}
