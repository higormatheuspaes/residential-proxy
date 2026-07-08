package main

import (
	"bufio"
	"database/sql"
	"encoding/base64"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/yamux"
	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------
// AgentPool: guarda todos os agentes (porteiros) conectados
// agora mesmo, e sabe escolher um deles em round-robin.
// ---------------------------------------------------------

type AgentPool struct {
	mu      sync.Mutex
	agents  map[string]*yamux.Session
	order   []string
	current int
}

func NewAgentPool() *AgentPool {
	return &AgentPool{agents: make(map[string]*yamux.Session)}
}

func (p *AgentPool) Add(id string, session *yamux.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.agents[id] = session
	p.order = append(p.order, id)
	log.Printf("Agente conectado: %s (total agora: %d)\n", id, len(p.agents))
}

func (p *AgentPool) Remove(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.agents, id)
	for i, aid := range p.order {
		if aid == id {
			p.order = append(p.order[:i], p.order[i+1:]...)
			break
		}
	}
	log.Printf("Agente desconectado: %s (total agora: %d)\n", id, len(p.agents))
}

func (p *AgentPool) Next() (string, *yamux.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.order) == 0 {
		return "", nil
	}
	p.current = (p.current + 1) % len(p.order)
	id := p.order[p.current]
	return id, p.agents[id]
}

// ---------------------------------------------------------
// ClientStore: autenticação de clientes, apoiada em SQLite.
// ---------------------------------------------------------

type ClientStore struct {
	db *sql.DB
}

func NewClientStore(path string) (*ClientStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS clients (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT UNIQUE NOT NULL,
		token TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}

	return &ClientStore{db: db}, nil
}

// Check confere usuario/token, e exige que o cliente esteja ativo.
func (c *ClientStore) Check(username, token string) bool {
	var storedToken string
	var active int

	row := c.db.QueryRow("SELECT token, active FROM clients WHERE username = ?", username)
	if err := row.Scan(&storedToken, &active); err != nil {
		return false // usuário não existe
	}

	return active == 1 && storedToken == token
}

// ---------------------------------------------------------
// Configurações
// ---------------------------------------------------------

const controlPort = ":7000"
const proxyPort = ":8080"

var sharedToken string

func main() {
	sharedToken = os.Getenv("RELAY_SHARED_TOKEN")
	if sharedToken == "" {
		log.Fatal("Defina RELAY_SHARED_TOKEN como variavel de ambiente antes de rodar.")
	}

	dbPath := os.Getenv("RELAY_DB_PATH")
	if dbPath == "" {
		dbPath = "relay.db"
	}

	store, err := NewClientStore(dbPath)
	if err != nil {
		log.Fatalf("Erro ao abrir banco de dados %s: %v", dbPath, err)
	}
	log.Printf("Banco de dados aberto em %s\n", dbPath)

	pool := NewAgentPool()
	log.Println("Relay iniciando...")

	go startControlListener(pool)
	go startProxyListener(pool, store)

	select {}
}

// ---------------------------------------------------------
// Parte 1: aceitar conexões de AGENTES na porta 7000
// ---------------------------------------------------------

func startControlListener(pool *AgentPool) {
	listener, err := net.Listen("tcp", controlPort)
	if err != nil {
		log.Fatalf("Erro ao escutar porta de controle %s: %v", controlPort, err)
	}
	log.Printf("Escutando agentes na porta %s\n", controlPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Erro ao aceitar conexão de agente:", err)
			continue
		}
		go handleAgentConnection(conn, pool)
	}
}

func handleAgentConnection(conn net.Conn, pool *AgentPool) {
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Falha ao ler handshake do agente:", err)
		conn.Close()
		return
	}
	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		log.Println("Handshake malformado, fechando conexão")
		conn.Close()
		return
	}

	token, agentID := parts[0], parts[1]
	if token != sharedToken {
		log.Println("Token inválido de um agente, recusando conexão")
		conn.Close()
		return
	}

	session, err := yamux.Server(conn, nil)
	if err != nil {
		log.Println("Erro ao criar sessão yamux com agente:", err)
		conn.Close()
		return
	}

	pool.Add(agentID, session)
	<-session.CloseChan()
	pool.Remove(agentID)
}

// ---------------------------------------------------------
// Parte 2: aceitar conexões do ROBÔ (HTTP proxy) na porta 8080
// ---------------------------------------------------------

func startProxyListener(pool *AgentPool, store *ClientStore) {
	listener, err := net.Listen("tcp", proxyPort)
	if err != nil {
		log.Fatalf("Erro ao escutar porta de proxy %s: %v", proxyPort, err)
	}
	log.Printf("Escutando robô (HTTP proxy) na porta %s\n", proxyPort)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Println("Erro ao aceitar conexão do robô:", err)
			continue
		}
		go handleProxyConnection(clientConn, pool, store)
	}
}

// checkProxyAuth confere o cabeçalho Proxy-Authorization contra o banco.
// Devolve o usuário autenticado (para logs) e se a autenticação foi ok.
func checkProxyAuth(req *http.Request, store *ClientStore) (string, bool) {
	authHeader := req.Header.Get("Proxy-Authorization")
	if authHeader == "" {
		return "", false
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || parts[0] != "Basic" {
		return "", false
	}

	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}

	credParts := strings.SplitN(string(decoded), ":", 2)
	if len(credParts) != 2 {
		return "", false
	}
	user, token := credParts[0], credParts[1]

	return user, store.Check(user, token)
}

type streamWithReader struct {
	io.Reader
	io.Writer
	io.Closer
}

func handleProxyConnection(clientConn net.Conn, pool *AgentPool, store *ClientStore) {
	defer clientConn.Close()

	reader := bufio.NewReader(clientConn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		log.Println("Erro ao ler requisição do robô:", err)
		return
	}

	user, ok := checkProxyAuth(req, store)
	if !ok {
		log.Println("Requisição sem autenticação válida, recusando")
		clientConn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n" +
			"Proxy-Authenticate: Basic realm=\"relay\"\r\n\r\n"))
		return
	}

	agentID, agentSession := pool.Next()
	if agentSession == nil {
		log.Println("Nenhum agente disponível no momento")
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\nNenhum agente conectado\r\n"))
		return
	}

	agentStream, err := agentSession.Open()
	if err != nil {
		log.Println("Erro ao abrir canal com o agente:", err)
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\nFalha ao abrir túnel\r\n"))
		return
	}
	defer agentStream.Close()

	agentReader := bufio.NewReader(agentStream)
	target := req.Host

	log.Printf("Cliente '%s' -> %s, atendido pelo agente: %s\n", user, target, agentID)

	if req.Method == http.MethodConnect {
		agentStream.Write([]byte("CONNECT " + target + "\n"))
	} else {
		agentStream.Write([]byte("HTTP " + target + "\n"))
		req.Write(agentStream)
	}

	ack, err := agentReader.ReadString('\n')
	if err != nil {
		log.Println("Erro ao ler confirmação do agente:", err)
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\nAgente não respondeu\r\n"))
		return
	}
	ack = strings.TrimSpace(ack)

	if ack != "OK" {
		log.Println("Agente falhou ao conectar no destino:", ack)
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n" + ack + "\r\n"))
		return
	}

	if req.Method == http.MethodConnect {
		clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	}

	wrappedAgent := streamWithReader{Reader: agentReader, Writer: agentStream, Closer: agentStream}
	pipe(clientConn, wrappedAgent)
}

func pipe(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }()
	go func() { io.Copy(b, a); done <- struct{}{} }()
	<-done
}