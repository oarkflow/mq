package mq

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// SecurityManager handles authentication, authorization, and security policies
type SecurityManager struct {
	authProviders  map[string]AuthProvider
	roleManager    *RoleManager
	rateLimiter    *SecurityRateLimiter
	auditLogger    *AuditLogger
	sessionManager *SessionManager
	encryptionKey  []byte
	mu             sync.RWMutex
}

// AuthProvider interface for different authentication methods
type AuthProvider interface {
	Name() string
	Authenticate(ctx context.Context, credentials map[string]interface{}) (*User, error)
	ValidateToken(token string) (*User, error)
}

// User represents an authenticated user
type User struct {
	ID          string                 `json:"id"`
	Username    string                 `json:"username"`
	Roles       []string               `json:"roles"`
	Permissions []string               `json:"permissions"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	LastLoginAt *time.Time             `json:"last_login_at,omitempty"`
}

// RoleManager manages user roles and permissions
type RoleManager struct {
	roles       map[string]*Role
	permissions map[string]*Permission
	mu          sync.RWMutex
}

// Role represents a user role with associated permissions
type Role struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
}

// Permission represents a specific permission
type Permission struct {
	Name        string    `json:"name"`
	Resource    string    `json:"resource"`
	Action      string    `json:"action"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// SecurityRateLimiter implements rate limiting for security operations
type SecurityRateLimiter struct {
	attempts    map[string]*RateLimitEntry
	maxAttempts int
	window      time.Duration
	mu          sync.RWMutex
}

// RateLimitEntry tracks rate limiting for a specific key
type RateLimitEntry struct {
	Count       int
	WindowStart time.Time
}

// AuditLogger logs security-related events
type AuditLogger struct {
	events    []AuditEvent
	maxEvents int
	mu        sync.RWMutex
}

// AuditEvent represents a security audit event
type AuditEvent struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	EventType string                 `json:"event_type"`
	UserID    string                 `json:"user_id,omitempty"`
	Resource  string                 `json:"resource"`
	Action    string                 `json:"action"`
	IPAddress string                 `json:"ip_address,omitempty"`
	UserAgent string                 `json:"user_agent,omitempty"`
	Success   bool                   `json:"success"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// SessionManager manages user sessions
type SessionManager struct {
	sessions map[string]*Session
	maxAge   time.Duration
	mu       sync.RWMutex
}

// Session represents a user session
type Session struct {
	ID        string                 `json:"id"`
	UserID    string                 `json:"user_id"`
	CreatedAt time.Time              `json:"created_at"`
	ExpiresAt time.Time              `json:"expires_at"`
	IPAddress string                 `json:"ip_address"`
	UserAgent string                 `json:"user_agent"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// NewSecurityManager creates a new security manager
func NewSecurityManager() *SecurityManager {
	key := make([]byte, 32)
	rand.Read(key)

	return &SecurityManager{
		authProviders:  make(map[string]AuthProvider),
		roleManager:    NewRoleManager(),
		rateLimiter:    NewSecurityRateLimiter(5, time.Minute*15), // 5 attempts per 15 minutes
		auditLogger:    NewAuditLogger(10000),
		sessionManager: NewSessionManager(time.Hour * 24), // 24 hour sessions
		encryptionKey:  key,
	}
}

// NewRoleManager creates a new role manager
func NewRoleManager() *RoleManager {
	rm := &RoleManager{
		roles:       make(map[string]*Role),
		permissions: make(map[string]*Permission),
	}

	// Initialize default permissions
	rm.AddPermission(&Permission{
		Name:        "task.publish",
		Resource:    "task",
		Action:      "publish",
		Description: "Publish tasks to queues",
		CreatedAt:   time.Now(),
	})

	rm.AddPermission(&Permission{
		Name:        "task.consume",
		Resource:    "task",
		Action:      "consume",
		Description: "Consume tasks from queues",
		CreatedAt:   time.Now(),
	})

	rm.AddPermission(&Permission{
		Name:        "queue.manage",
		Resource:    "queue",
		Action:      "manage",
		Description: "Manage queues",
		CreatedAt:   time.Now(),
	})

	rm.AddPermission(&Permission{
		Name:        "admin.system",
		Resource:    "system",
		Action:      "admin",
		Description: "System administration",
		CreatedAt:   time.Now(),
	})

	// Initialize default roles
	rm.AddRole(&Role{
		Name:        "publisher",
		Description: "Can publish tasks",
		Permissions: []string{"task.publish"},
		CreatedAt:   time.Now(),
	})

	rm.AddRole(&Role{
		Name:        "consumer",
		Description: "Can consume tasks",
		Permissions: []string{"task.consume"},
		CreatedAt:   time.Now(),
	})

	rm.AddRole(&Role{
		Name:        "admin",
		Description: "Full system access",
		Permissions: []string{"task.publish", "task.consume", "queue.manage", "admin.system"},
		CreatedAt:   time.Now(),
	})

	return rm
}

// AddPermission adds a permission to the role manager
func (rm *RoleManager) AddPermission(perm *Permission) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.permissions[perm.Name] = perm
}

// AddRole adds a role to the role manager
func (rm *RoleManager) AddRole(role *Role) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.roles[role.Name] = role
}

// HasPermission checks if a user has a specific permission
func (rm *RoleManager) HasPermission(user *User, permission string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, roleName := range user.Roles {
		if role, exists := rm.roles[roleName]; exists {
			for _, perm := range role.Permissions {
				if perm == permission {
					return true
				}
			}
		}
	}
	return false
}

// GetUserPermissions returns all permissions for a user
func (rm *RoleManager) GetUserPermissions(user *User) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	permissions := make(map[string]bool)
	for _, roleName := range user.Roles {
		if role, exists := rm.roles[roleName]; exists {
			for _, perm := range role.Permissions {
				permissions[perm] = true
			}
		}
	}

	result := make([]string, 0, len(permissions))
	for perm := range permissions {
		result = append(result, perm)
	}
	return result
}

// NewSecurityRateLimiter creates a new security rate limiter
func NewSecurityRateLimiter(maxAttempts int, window time.Duration) *SecurityRateLimiter {
	return &SecurityRateLimiter{
		attempts:    make(map[string]*RateLimitEntry),
		maxAttempts: maxAttempts,
		window:      window,
	}
}

// IsAllowed checks if an action is allowed based on rate limiting
func (rl *SecurityRateLimiter) IsAllowed(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	entry, exists := rl.attempts[key]

	if !exists {
		rl.attempts[key] = &RateLimitEntry{
			Count:       1,
			WindowStart: now,
		}
		return true
	}

	// Check if we're in a new window
	if now.Sub(entry.WindowStart) >= rl.window {
		entry.Count = 1
		entry.WindowStart = now
		return true
	}

	// Check if we've exceeded the limit
	if entry.Count >= rl.maxAttempts {
		return false
	}

	entry.Count++
	return true
}

// Reset resets the rate limit for a key
func (rl *SecurityRateLimiter) Reset(key string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, key)
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(maxEvents int) *AuditLogger {
	return &AuditLogger{
		events:    make([]AuditEvent, 0),
		maxEvents: maxEvents,
	}
}

// LogEvent logs a security event
func (al *AuditLogger) LogEvent(event AuditEvent) {
	al.mu.Lock()
	defer al.mu.Unlock()

	event.ID = generateID()
	event.Timestamp = time.Now()

	al.events = append(al.events, event)

	// Keep only the most recent events
	if len(al.events) > al.maxEvents {
		al.events = al.events[len(al.events)-al.maxEvents:]
	}
}

// GetEvents returns audit events with optional filtering
func (al *AuditLogger) GetEvents(userID, eventType string, limit int) []AuditEvent {
	al.mu.RLock()
	defer al.mu.RUnlock()

	var filtered []AuditEvent
	for _, event := range al.events {
		if (userID == "" || event.UserID == userID) &&
			(eventType == "" || event.EventType == eventType) {
			filtered = append(filtered, event)
		}
	}

	// Return most recent events
	if len(filtered) > limit && limit > 0 {
		start := len(filtered) - limit
		return filtered[start:]
	}

	return filtered
}

// NewSessionManager creates a new session manager
func NewSessionManager(maxAge time.Duration) *SessionManager {
	sm := &SessionManager{
		sessions: make(map[string]*Session),
		maxAge:   maxAge,
	}

	// Start cleanup routine
	go sm.cleanupRoutine()

	return sm
}

// CreateSession creates a new session for a user
func (sm *SessionManager) CreateSession(userID, ipAddress, userAgent string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session := &Session{
		ID:        generateID(),
		UserID:    userID,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(sm.maxAge),
		IPAddress: ipAddress,
		UserAgent: userAgent,
		Data:      make(map[string]interface{}),
	}

	sm.sessions[session.ID] = session
	return session
}

// GetSession retrieves a session by ID
func (sm *SessionManager) GetSession(sessionID string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, exists := sm.sessions[sessionID]
	if !exists {
		return nil, false
	}

	// Check if session has expired
	if time.Now().After(session.ExpiresAt) {
		return nil, false
	}

	return session, true
}

// DeleteSession deletes a session
func (sm *SessionManager) DeleteSession(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, sessionID)
}

// cleanupRoutine periodically cleans up expired sessions
func (sm *SessionManager) cleanupRoutine() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		sm.mu.Lock()
		now := time.Now()
		for id, session := range sm.sessions {
			if now.After(session.ExpiresAt) {
				delete(sm.sessions, id)
			}
		}
		sm.mu.Unlock()
	}
}

// AddAuthProvider adds an authentication provider
func (sm *SecurityManager) AddAuthProvider(provider AuthProvider) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.authProviders[provider.Name()] = provider
}

// Authenticate authenticates a user using available providers
func (sm *SecurityManager) Authenticate(ctx context.Context, credentials map[string]interface{}) (*User, error) {
	sm.mu.RLock()
	providers := make(map[string]AuthProvider)
	for name, provider := range sm.authProviders {
		providers[name] = provider
	}
	sm.mu.RUnlock()

	var lastErr error
	for _, provider := range providers {
		user, err := provider.Authenticate(ctx, credentials)
		if err == nil {
			// Log successful authentication
			sm.auditLogger.LogEvent(AuditEvent{
				EventType: "authentication",
				UserID:    user.ID,
				Action:    "login",
				Success:   true,
				Details: map[string]interface{}{
					"provider": provider.Name(),
				},
			})

			// Update user permissions
			user.Permissions = sm.roleManager.GetUserPermissions(user)
			return user, nil
		}
		lastErr = err
	}

	// Log failed authentication
	sm.auditLogger.LogEvent(AuditEvent{
		EventType: "authentication",
		Action:    "login",
		Success:   false,
		Details: map[string]interface{}{
			"error": lastErr.Error(),
		},
	})

	return nil, fmt.Errorf("authentication failed: %w", lastErr)
}

// Authorize checks if a user is authorized for an action
func (sm *SecurityManager) Authorize(user *User, resource, action string) error {
	permission := fmt.Sprintf("%s.%s", resource, action)

	if !sm.roleManager.HasPermission(user, permission) {
		// Log authorization failure
		sm.auditLogger.LogEvent(AuditEvent{
			EventType: "authorization",
			UserID:    user.ID,
			Resource:  resource,
			Action:    action,
			Success:   false,
		})

		return fmt.Errorf("user %s does not have permission %s", user.Username, permission)
	}

	// Log successful authorization
	sm.auditLogger.LogEvent(AuditEvent{
		EventType: "authorization",
		UserID:    user.ID,
		Resource:  resource,
		Action:    action,
		Success:   true,
	})

	return nil
}

// ValidateSession validates a session token
func (sm *SecurityManager) ValidateSession(sessionID string) (*User, error) {
	session, exists := sm.sessionManager.GetSession(sessionID)
	if !exists {
		return nil, fmt.Errorf("invalid session")
	}

	// Create a user object from session data
	user := &User{
		ID:       session.UserID,
		Username: session.UserID, // In a real implementation, you'd fetch from database
	}

	// Update user permissions
	user.Permissions = sm.roleManager.GetUserPermissions(user)

	return user, nil
}

// CheckRateLimit checks if an action is rate limited
func (sm *SecurityManager) CheckRateLimit(key string) error {
	if !sm.rateLimiter.IsAllowed(key) {
		sm.auditLogger.LogEvent(AuditEvent{
			EventType: "rate_limit",
			Action:    "exceeded",
			Success:   false,
			Details: map[string]interface{}{
				"key": key,
			},
		})

		return fmt.Errorf("rate limit exceeded for key: %s", key)
	}

	return nil
}

// Encrypt encrypts data using the security manager's key
func (sm *SecurityManager) Encrypt(data []byte) ([]byte, error) {
	// Simple encryption using SHA256 hash of key + data
	// In production, use proper encryption like AES
	hash := sha256.Sum256(append(sm.encryptionKey, data...))
	return hash[:], nil
}

// Decrypt decrypts data (placeholder for proper decryption)
func (sm *SecurityManager) Decrypt(data []byte) ([]byte, error) {
	// Placeholder - in production, implement proper decryption
	return data, nil
}

// BasicAuthProvider implements basic username/password authentication
type BasicAuthProvider struct {
	users map[string]*User
	mu    sync.RWMutex
}

func NewBasicAuthProvider() *BasicAuthProvider {
	return &BasicAuthProvider{
		users: make(map[string]*User),
	}
}

func (bap *BasicAuthProvider) Name() string {
	return "basic"
}

func (bap *BasicAuthProvider) Authenticate(ctx context.Context, credentials map[string]interface{}) (*User, error) {
	username, ok := credentials["username"].(string)
	if !ok {
		return nil, fmt.Errorf("username required")
	}

	password, ok := credentials["password"].(string)
	if !ok {
		return nil, fmt.Errorf("password required")
	}

	bap.mu.RLock()
	user, exists := bap.users[username]
	bap.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("user not found")
	}

	// In production, compare hashed passwords
	if password != "password" { // Placeholder
		return nil, fmt.Errorf("invalid password")
	}

	// Update last login
	now := time.Now()
	user.LastLoginAt = &now

	return user, nil
}

func (bap *BasicAuthProvider) ValidateToken(token string) (*User, error) {
	// Basic token validation - in production, use JWT or similar
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token format")
	}

	username := parts[0]
	return bap.Authenticate(context.Background(), map[string]interface{}{
		"username": username,
		"password": "token", // Placeholder
	})
}

func (bap *BasicAuthProvider) AddUser(user *User, password string) error {
	bap.mu.Lock()
	defer bap.mu.Unlock()

	// In production, hash the password
	user.CreatedAt = time.Now()
	bap.users[user.Username] = user

	return nil
}

// generateID generates a random ID
func generateID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// SecurityMiddleware provides security middleware for HTTP handlers
type SecurityMiddleware struct {
	securityManager *SecurityManager
}

// NewSecurityMiddleware creates a new security middleware
func NewSecurityMiddleware(sm *SecurityManager) *SecurityMiddleware {
	return &SecurityMiddleware{
		securityManager: sm,
	}
}

// AuthenticateRequest authenticates a request with credentials
func (sm *SecurityMiddleware) AuthenticateRequest(credentials map[string]interface{}, ipAddress string) (*User, error) {
	user, err := sm.securityManager.Authenticate(context.Background(), credentials)
	if err != nil {
		// Log failed authentication attempt
		sm.securityManager.auditLogger.LogEvent(AuditEvent{
			EventType: "authentication",
			Action:    "login",
			Success:   false,
			Details: map[string]interface{}{
				"ip_address": ipAddress,
				"error":      err.Error(),
			},
		})
		return nil, err
	}

	return user, nil
}

// AuthorizeRequest authorizes a request
func (sm *SecurityMiddleware) AuthorizeRequest(user *User, resource, action string) error {
	return sm.securityManager.Authorize(user, resource, action)
}

// GetClientIP gets the real client IP from a network connection
func GetClientIP(conn net.Conn) string {
	if conn == nil {
		return ""
	}

	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}

	return host
}
