package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
)

// hzSseSession represents an active SSE connection.
type hzSseSession struct {
	writer              *app.RequestContext
	done                chan struct{}
	eventQueue          chan string // Channel for queuing events
	sessionID           string
	requestID           atomic.Int64
	notificationChannel chan mcp.JSONRPCNotification
	initialized         atomic.Bool
}

// SSEContextFunc is a function that takes an existing context and the current
// request and returns a potentially modified context based on the request
// content. This can be used to inject context values from headers, for example.
type HzSSEContextFunc func(ctx context.Context, c *app.RequestContext) context.Context

// DynamicBasePathFunc allows the user to provide a function to generate the
// base path for a given request and sessionID. This is useful for cases where
// the base path is not known at the time of SSE server creation, such as when
// using a reverse proxy or when the base path is dynamically generated. The
// function should return the base path (e.g., "/mcp/tenant123").
type HzDynamicBasePathFunc func(ctx context.Context, c *app.RequestContext, sessionID string) string

func (s *hzSseSession) SessionID() string {
	return s.sessionID
}

func (s *hzSseSession) NotificationChannel() chan<- mcp.JSONRPCNotification {
	return s.notificationChannel
}

func (s *hzSseSession) Initialize() {
	s.initialized.Store(true)
}

func (s *hzSseSession) Initialized() bool {
	return s.initialized.Load()
}

var _ ClientSession = (*hzSseSession)(nil)

// SSEServer implements a Server-Sent Events (SSE) based MCP server.
// It provides real-time communication capabilities over HTTP using the SSE protocol.
type HzSSEServer struct {
	server                       *MCPServer
	baseURL                      string
	basePath                     string
	appendQueryToMessageEndpoint bool
	useFullURLForMessageEndpoint bool
	messageEndpoint              string
	sseEndpoint                  string
	sessions                     sync.Map
	srv                          *server.Hertz
	contextFunc                  HzSSEContextFunc
	dynamicBasePathFunc          HzDynamicBasePathFunc

	keepAlive         bool
	keepAliveInterval time.Duration

	mu sync.RWMutex
}

// SSEOption defines a function type for configuring SSEServer
type HzSSEOption func(*HzSSEServer)

// WithBaseURL sets the base URL for the SSE server
func WithHzBaseURL(baseURL string) HzSSEOption {
	return func(s *HzSSEServer) {
		if baseURL != "" {
			u, err := url.Parse(baseURL)
			if err != nil {
				return
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				return
			}
			// Check if the host is empty or only contains a port
			if u.Host == "" || strings.HasPrefix(u.Host, ":") {
				return
			}
			if len(u.Query()) > 0 {
				return
			}
		}
		s.baseURL = strings.TrimSuffix(baseURL, "/")
	}
}

// WithHzBasePath adds a new option for setting a static base path
func WithHzBasePath(basePath string) HzSSEOption {
	return func(s *HzSSEServer) {
		s.basePath = normalizeURLPath(basePath)
	}
}

// WithHzDynamicBasePath accepts a function for generating the base path. This is
// useful for cases where the base path is not known at the time of SSE server
// creation, such as when using a reverse proxy or when the server is mounted
// at a dynamic path.
func WithHzDynamicBasePath(fn HzDynamicBasePathFunc) HzSSEOption {
	return func(s *HzSSEServer) {
		if fn != nil {
			s.dynamicBasePathFunc = func(ctx context.Context, c *app.RequestContext, sid string) string {
				bp := fn(ctx, c, sid)
				return normalizeURLPath(bp)
			}
		}
	}
}

// WithHzMessageEndpoint sets the message endpoint path
func WithHzMessageEndpoint(endpoint string) HzSSEOption {
	return func(s *HzSSEServer) {
		s.messageEndpoint = endpoint
	}
}

// WithAppendQueryToMessageEndpoint configures the SSE server to append the original request's
// query parameters to the message endpoint URL that is sent to clients during the SSE connection
// initialization. This is useful when you need to preserve query parameters from the initial
// SSE connection request and carry them over to subsequent message requests, maintaining
// context or authentication details across the communication channel.
func WithHzAppendQueryToMessageEndpoint() HzSSEOption {
	return func(s *HzSSEServer) {
		s.appendQueryToMessageEndpoint = true
	}
}

// WithUseFullURLForMessageEndpoint controls whether the SSE server returns a complete URL (including baseURL)
// or just the path portion for the message endpoint. Set to false when clients will concatenate
// the baseURL themselves to avoid malformed URLs like "http://localhost/mcphttp://localhost/mcp/message".
func WithHzUseFullURLForMessageEndpoint(useFullURLForMessageEndpoint bool) HzSSEOption {
	return func(s *HzSSEServer) {
		s.useFullURLForMessageEndpoint = useFullURLForMessageEndpoint
	}
}

// WithSSEEndpoint sets the SSE endpoint path
func WithHzSSEEndpoint(endpoint string) HzSSEOption {
	return func(s *HzSSEServer) {
		s.sseEndpoint = endpoint
	}
}

// WithHertzServer sets the Hertz server instance
func WithHertzServer(srv *server.Hertz) HzSSEOption {
	return func(s *HzSSEServer) {
		s.srv = srv
	}
}

func WithHzKeepAliveInterval(keepAliveInterval time.Duration) HzSSEOption {
	return func(s *HzSSEServer) {
		s.keepAlive = true
		s.keepAliveInterval = keepAliveInterval
	}
}

func WithHzKeepAlive(keepAlive bool) HzSSEOption {
	return func(s *HzSSEServer) {
		s.keepAlive = keepAlive
	}
}

// WithHzSSEContextFunc sets a function that will be called to customise the context
// to the server using the incoming request.
func WithHzSSEContextFunc(fn HzSSEContextFunc) HzSSEOption {
	return func(s *HzSSEServer) {
		s.contextFunc = fn
	}
}

// NewHzSSEServer creates a new SSE server instance with the given MCP server and options.
func NewHzSSEServer(server *MCPServer, opts ...HzSSEOption) *HzSSEServer {
	s := &HzSSEServer{
		server:                       server,
		sseEndpoint:                  "/sse",
		messageEndpoint:              "/message",
		useFullURLForMessageEndpoint: true,
		keepAlive:                    false,
		keepAliveInterval:            10 * time.Second,
	}

	// Apply all options
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Start begins serving SSE connections on the specified address.
// It sets up HTTP handlers for SSE and message endpoints.
func (s *HzSSEServer) RegisterMCPServer(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.srv == nil {
		s.srv = server.Default(
			server.WithHostPorts(addr),
		)
	}

	// Register routes
	ssePath := s.CompleteSsePath()
	messagePath := s.CompleteMessagePath()

	s.srv.GET(ssePath, func(ctx context.Context, c *app.RequestContext) {
		s.handleSSE(ctx, c)
	})
	s.srv.POST(messagePath, func(ctx context.Context, c *app.RequestContext) {
		s.handleMessage(ctx, c)
	})
}

// Start begins serving SSE connections on the specified address.
// It sets up HTTP handlers for SSE and message endpoints.
func (s *HzSSEServer) Start(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.srv == nil {
		s.srv = server.Default(
			server.WithHostPorts(addr),
		)
	}

	// Register routes
	ssePath := s.CompleteSsePath()
	messagePath := s.CompleteMessagePath()

	s.srv.GET(ssePath, func(ctx context.Context, c *app.RequestContext) {
		s.handleSSE(ctx, c)
	})
	s.srv.POST(messagePath, func(ctx context.Context, c *app.RequestContext) {
		s.handleMessage(ctx, c)
	})

	s.srv.Spin()
}

// Shutdown gracefully stops the SSE server, closing all active sessions
// and shutting down the HTTP server.
func (s *HzSSEServer) Shutdown(ctx context.Context) error {
	s.mu.RLock()
	srv := s.srv
	s.mu.RUnlock()

	if srv != nil {
		s.sessions.Range(func(key, value interface{}) bool {
			if session, ok := value.(*sseSession); ok {
				close(session.done)
			}
			s.sessions.Delete(key)
			return true
		})

		return srv.Shutdown(ctx)
	}
	return nil
}

// handleSSE handles incoming SSE connection requests.
// It sets up appropriate headers and creates a new session for the client.
func (s *HzSSEServer) handleSSE(ctx context.Context, c *app.RequestContext) {
	if string(c.Request.Method()) != consts.MethodGet {
		c.String(consts.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	sessionID := uuid.New().String()
	session := &hzSseSession{
		writer:              c,
		done:                make(chan struct{}),
		eventQueue:          make(chan string, 100), // Buffer for events
		sessionID:           sessionID,
		notificationChannel: make(chan mcp.JSONRPCNotification, 100),
	}

	s.sessions.Store(sessionID, session)
	defer s.sessions.Delete(sessionID)

	if err := s.server.RegisterSession(ctx, session); err != nil {
		c.String(consts.StatusInternalServerError, fmt.Sprintf("Session registration failed: %v", err))
		return
	}
	defer s.server.UnregisterSession(ctx, sessionID)

	// Start notification handler for this session
	go func() {
		for {
			select {
			case notification := <-session.notificationChannel:
				eventData, err := json.Marshal(notification)
				if err == nil {
					select {
					case session.eventQueue <- fmt.Sprintf("event: message\ndata: %s\n\n", eventData):
						// Event queued successfully
					case <-session.done:
						return
					}
				}
			case <-session.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start keep alive : ping
	if s.keepAlive {
		go func() {
			ticker := time.NewTicker(s.keepAliveInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					message := mcp.JSONRPCRequest{
						JSONRPC: "2.0",
						ID:      session.requestID.Add(1),
						Request: mcp.Request{
							Method: "ping",
						},
					}
					messageBytes, _ := json.Marshal(message)
					pingMsg := fmt.Sprintf("event: message\ndata:%s\n\n", messageBytes)
					session.eventQueue <- pingMsg
				case <-session.done:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Send the initial endpoint event
	endpoint := s.GetMessageEndpointForClient(ctx, c, sessionID)
	if s.appendQueryToMessageEndpoint && len(c.QueryArgs().String()) > 0 {
		endpoint += "&" + c.QueryArgs().String()
	}
	c.WriteString(fmt.Sprintf("event: endpoint\ndata: %s\r\n\r\n", endpoint))
	c.Flush()

	// Main event loop - this runs in the HTTP handler goroutine
	for {
		select {
		case event := <-session.eventQueue:
			// Write the event to the response
			c.WriteString(event)
			c.Flush()
		case <-ctx.Done():
			close(session.done)
			return
		case <-session.done:
			return
		}
	}
}

// GetMessageEndpointForClient returns the appropriate message endpoint URL with session ID
// for the given request. This is the canonical way to compute the message endpoint for a client.
// It handles both dynamic and static path modes, and honors the WithUseFullURLForMessageEndpoint flag.
func (s *HzSSEServer) GetMessageEndpointForClient(ctx context.Context, c *app.RequestContext, sessionID string) string {
	basePath := s.basePath
	if s.dynamicBasePathFunc != nil {
		basePath = s.dynamicBasePathFunc(ctx, c, sessionID)
	}

	endpointPath := normalizeURLPath(basePath, s.messageEndpoint)
	if s.useFullURLForMessageEndpoint && s.baseURL != "" {
		endpointPath = s.baseURL + endpointPath
	}

	return fmt.Sprintf("%s?sessionId=%s", endpointPath, sessionID)
}

// handleMessage processes incoming JSON-RPC messages from clients and sends responses
// back through the SSE connection and 202 code to HTTP response.
func (s *HzSSEServer) handleMessage(ctx context.Context, c *app.RequestContext) {
	if string(c.Request.Method()) != consts.MethodPost {
		s.writeJSONRPCError(c, nil, mcp.INVALID_REQUEST, "Method not allowed")
		return
	}

	sessionID := c.Query("sessionId")
	if sessionID == "" {
		s.writeJSONRPCError(c, nil, mcp.INVALID_PARAMS, "Missing sessionId")
		return
	}
	sessionI, ok := s.sessions.Load(sessionID)
	if !ok {
		s.writeJSONRPCError(c, nil, mcp.INVALID_PARAMS, "Invalid session ID")
		return
	}
	session := sessionI.(*sseSession)

	// Set the client context before handling the message
	ctx = s.server.WithContext(ctx, session)
	if s.contextFunc != nil {
		ctx = s.contextFunc(ctx, c)
	}

	// Parse message as raw JSON
	var rawMessage json.RawMessage
	if err := c.BindJSON(&rawMessage); err != nil {
		s.writeJSONRPCError(c, nil, mcp.PARSE_ERROR, "Parse error")
		return
	}

	// quick return request, send 202 Accepted with no body, then deal the message and sent response via SSE
	c.Status(consts.StatusAccepted)

	go func() {
		// Process message through MCPServer
		response := s.server.HandleMessage(ctx, rawMessage)

		// Only send response if there is one (not for notifications)
		if response != nil {
			var message string
			if eventData, err := json.Marshal(response); err != nil {
				// If there is an error marshalling the response, send a generic error response
				log.Printf("failed to marshal response: %v", err)
				message = fmt.Sprintf("event: message\ndata: {\"error\": \"internal error\",\"jsonrpc\": \"2.0\", \"id\": null}\n\n")
				return
			} else {
				message = fmt.Sprintf("event: message\ndata: %s\n\n", eventData)
			}

			// Queue the event for sending via SSE
			select {
			case session.eventQueue <- message:
				// Event queued successfully
			case <-session.done:
				// Session is closed, don't try to queue
			default:
				// Queue is full, log this situation
				log.Printf("Event queue full for session %s", sessionID)
			}
		}
	}()
}

// writeJSONRPCError writes a JSON-RPC error response with the given error details.
func (s *HzSSEServer) writeJSONRPCError(
	c *app.RequestContext,
	id interface{},
	code int,
	message string,
) {
	response := createErrorResponse(id, code, message)
	c.Header("Content-Type", "application/json")
	c.Status(consts.StatusBadRequest)
	c.JSON(consts.StatusBadRequest, response)
}

// SendEventToSession sends an event to a specific SSE session identified by sessionID.
// Returns an error if the session is not found or closed.
func (s *HzSSEServer) SendEventToSession(
	sessionID string,
	event interface{},
) error {
	sessionI, ok := s.sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	session := sessionI.(*sseSession)

	eventData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Queue the event for sending via SSE
	select {
	case session.eventQueue <- fmt.Sprintf("event: message\ndata: %s\n\n", eventData):
		return nil
	case <-session.done:
		return fmt.Errorf("session closed")
	default:
		return fmt.Errorf("event queue full")
	}
}

func (s *HzSSEServer) GetUrlPath(input string) (string, error) {
	parse, err := url.Parse(input)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL %s: %w", input, err)
	}
	return parse.Path, nil
}

func (s *HzSSEServer) CompleteSseEndpoint() (string, error) {
	if s.dynamicBasePathFunc != nil {
		return "", &ErrDynamicPathConfig{Method: "CompleteSseEndpoint"}
	}

	path := normalizeURLPath(s.basePath, s.sseEndpoint)
	return s.baseURL + path, nil
}

func (s *HzSSEServer) CompleteSsePath() string {
	path, err := s.CompleteSseEndpoint()
	if err != nil {
		return normalizeURLPath(s.basePath, s.sseEndpoint)
	}
	urlPath, err := s.GetUrlPath(path)
	if err != nil {
		return normalizeURLPath(s.basePath, s.sseEndpoint)
	}
	return urlPath
}

func (s *HzSSEServer) CompleteMessageEndpoint() (string, error) {
	if s.dynamicBasePathFunc != nil {
		return "", &ErrDynamicPathConfig{Method: "CompleteMessageEndpoint"}
	}
	path := normalizeURLPath(s.basePath, s.messageEndpoint)
	return s.baseURL + path, nil
}

func (s *HzSSEServer) CompleteMessagePath() string {
	path, err := s.CompleteMessageEndpoint()
	if err != nil {
		return normalizeURLPath(s.basePath, s.messageEndpoint)
	}
	urlPath, err := s.GetUrlPath(path)
	if err != nil {
		return normalizeURLPath(s.basePath, s.messageEndpoint)
	}
	return urlPath
}
