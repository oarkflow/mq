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

	"github.com/oarkflow/squealx"
)

// Storage interfaces for persistence
type UserStorage interface {
	GetUser(username string) (*User, error)
	GetPassword(username string) (string, error)
	SaveUser(user *User, password string) error
	ListUsers() ([]*User, error)
	DeleteUser(username string) error
}

type RoleStorage interface {
	GetRole(name string) (*Role, error)
	SaveRole(role *Role) error
	ListRoles() ([]*Role, error)
	DeleteRole(name string) error
}

type PermissionStorage interface {
	GetPermission(name string) (*Permission, error)
	SavePermission(perm *Permission) error
	ListPermissions() ([]*Permission, error)
	DeletePermission(name string) error
}

// MemoryUserStorage implements UserStorage using in-memory storage
type MemoryUserStorage struct {
	users     map[string]*User
	passwords map[string]string
	mu        sync.RWMutex
}

func NewMemoryUserStorage() *MemoryUserStorage {
	return &MemoryUserStorage{
		users:     make(map[string]*User),
		passwords: make(map[string]string),
	}
}

func (mus *MemoryUserStorage) GetUser(username string) (*User, error) {
	mus.mu.RLock()
	defer mus.mu.RUnlock()
	user, exists := mus.users[username]
	if !exists {
		return nil, fmt.Errorf("user not found")
	}
	return user, nil
}

func (mus *MemoryUserStorage) SaveUser(user *User, password string) error {
	mus.mu.Lock()
	defer mus.mu.Unlock()
	mus.users[user.Username] = user
	mus.passwords[user.Username] = password
	return nil
}

func (mus *MemoryUserStorage) ListUsers() ([]*User, error) {
	mus.mu.RLock()
	defer mus.mu.RUnlock()
	users := make([]*User, 0, len(mus.users))
	for _, user := range mus.users {
		users = append(users, user)
	}
	return users, nil
}

func (mus *MemoryUserStorage) GetPassword(username string) (string, error) {
	mus.mu.RLock()
	defer mus.mu.RUnlock()
	password, exists := mus.passwords[username]
	if !exists {
		return "", fmt.Errorf("password not found")
	}
	return password, nil
}

func (mus *MemoryUserStorage) DeleteUser(username string) error {
	mus.mu.Lock()
	defer mus.mu.Unlock()
	delete(mus.users, username)
	delete(mus.passwords, username)
	return nil
}

// MemoryRoleStorage implements RoleStorage using in-memory storage
type MemoryRoleStorage struct {
	roles map[string]*Role
	mu    sync.RWMutex
}

func NewMemoryRoleStorage() *MemoryRoleStorage {
	return &MemoryRoleStorage{
		roles: make(map[string]*Role),
	}
}

func (mrs *MemoryRoleStorage) GetRole(name string) (*Role, error) {
	mrs.mu.RLock()
	defer mrs.mu.RUnlock()
	role, exists := mrs.roles[name]
	if !exists {
		return nil, fmt.Errorf("role not found")
	}
	return role, nil
}

func (mrs *MemoryRoleStorage) SaveRole(role *Role) error {
	mrs.mu.Lock()
	defer mrs.mu.Unlock()
	mrs.roles[role.Name] = role
	return nil
}

func (mrs *MemoryRoleStorage) ListRoles() ([]*Role, error) {
	mrs.mu.RLock()
	defer mrs.mu.RUnlock()
	roles := make([]*Role, 0, len(mrs.roles))
	for _, role := range mrs.roles {
		roles = append(roles, role)
	}
	return roles, nil
}

func (mrs *MemoryRoleStorage) DeleteRole(name string) error {
	mrs.mu.Lock()
	defer mrs.mu.Unlock()
	delete(mrs.roles, name)
	return nil
}

// MemoryPermissionStorage implements PermissionStorage using in-memory storage
type MemoryPermissionStorage struct {
	permissions map[string]*Permission
	mu          sync.RWMutex
}

func NewMemoryPermissionStorage() *MemoryPermissionStorage {
	return &MemoryPermissionStorage{
		permissions: make(map[string]*Permission),
	}
}

func (mps *MemoryPermissionStorage) GetPermission(name string) (*Permission, error) {
	mps.mu.RLock()
	defer mps.mu.RUnlock()
	perm, exists := mps.permissions[name]
	if !exists {
		return nil, fmt.Errorf("permission not found")
	}
	return perm, nil
}

func (mps *MemoryPermissionStorage) SavePermission(perm *Permission) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	mps.permissions[perm.Name] = perm
	return nil
}

func (mps *MemoryPermissionStorage) ListPermissions() ([]*Permission, error) {
	mps.mu.RLock()
	defer mps.mu.RUnlock()
	perms := make([]*Permission, 0, len(mps.permissions))
	for _, perm := range mps.permissions {
		perms = append(perms, perm)
	}
	return perms, nil
}

func (mps *MemoryPermissionStorage) DeletePermission(name string) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	delete(mps.permissions, name)
	return nil
}

// SQLUserStorage implements UserStorage using SQL database
type SQLUserStorage struct {
	db *squealx.DB
}

func NewSQLUserStorage(db *squealx.DB) *SQLUserStorage {
	return &SQLUserStorage{db: db}
}

func (sus *SQLUserStorage) GetUser(username string) (*User, error) {
	var user User
	err := sus.db.Get(&user, "SELECT id, username, roles, permissions, metadata, created_at, last_login_at FROM users WHERE username = $1", username)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (sus *SQLUserStorage) GetPassword(username string) (string, error) {
	var password string
	err := sus.db.Get(&password, "SELECT password FROM users WHERE username = $1", username)
	if err != nil {
		return "", err
	}
	return password, nil
}

func (sus *SQLUserStorage) SaveUser(user *User, password string) error {
	_, err := sus.db.Exec(`
		INSERT INTO users (id, username, password, roles, permissions, metadata, created_at, last_login_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (username) DO UPDATE SET
			password = EXCLUDED.password,
			roles = EXCLUDED.roles,
			permissions = EXCLUDED.permissions,
			metadata = EXCLUDED.metadata,
			last_login_at = EXCLUDED.last_login_at`,
		user.ID, user.Username, password, user.Roles, user.Permissions, user.Metadata, user.CreatedAt, user.LastLoginAt)
	return err
}

func (sus *SQLUserStorage) ListUsers() ([]*User, error) {
	var users []*User
	err := sus.db.Select(&users, "SELECT id, username, roles, permissions, metadata, created_at, last_login_at FROM users")
	return users, err
}

func (sus *SQLUserStorage) DeleteUser(username string) error {
	_, err := sus.db.Exec("DELETE FROM users WHERE username = $1", username)
	return err
}

// SQLRoleStorage implements RoleStorage using SQL database
type SQLRoleStorage struct {
	db *squealx.DB
}

func NewSQLRoleStorage(db *squealx.DB) *SQLRoleStorage {
	return &SQLRoleStorage{db: db}
}

func (srs *SQLRoleStorage) GetRole(name string) (*Role, error) {
	var role Role
	err := srs.db.Get(&role, "SELECT name, description, permissions, created_at FROM roles WHERE name = $1", name)
	if err != nil {
		return nil, err
	}
	return &role, nil
}

func (srs *SQLRoleStorage) SaveRole(role *Role) error {
	_, err := srs.db.Exec(`
		INSERT INTO roles (name, description, permissions, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET
			description = EXCLUDED.description,
			permissions = EXCLUDED.permissions`,
		role.Name, role.Description, role.Permissions, role.CreatedAt)
	return err
}

func (srs *SQLRoleStorage) ListRoles() ([]*Role, error) {
	var roles []*Role
	err := srs.db.Select(&roles, "SELECT name, description, permissions, created_at FROM roles")
	return roles, err
}

func (srs *SQLRoleStorage) DeleteRole(name string) error {
	_, err := srs.db.Exec("DELETE FROM roles WHERE name = $1", name)
	return err
}

// SQLPermissionStorage implements PermissionStorage using SQL database
type SQLPermissionStorage struct {
	db *squealx.DB
}

func NewSQLPermissionStorage(db *squealx.DB) *SQLPermissionStorage {
	return &SQLPermissionStorage{db: db}
}

func (sps *SQLPermissionStorage) GetPermission(name string) (*Permission, error) {
	var perm Permission
	err := sps.db.Get(&perm, "SELECT name, resource, action, description, created_at FROM permissions WHERE name = $1", name)
	if err != nil {
		return nil, err
	}
	return &perm, nil
}

func (sps *SQLPermissionStorage) SavePermission(perm *Permission) error {
	_, err := sps.db.Exec(`
		INSERT INTO permissions (name, resource, action, description, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE SET
			resource = EXCLUDED.resource,
			action = EXCLUDED.action,
			description = EXCLUDED.description`,
		perm.Name, perm.Resource, perm.Action, perm.Description, perm.CreatedAt)
	return err
}

func (sps *SQLPermissionStorage) ListPermissions() ([]*Permission, error) {
	var perms []*Permission
	err := sps.db.Select(&perms, "SELECT name, resource, action, description, created_at FROM permissions")
	return perms, err
}

func (sps *SQLPermissionStorage) DeletePermission(name string) error {
	_, err := sps.db.Exec("DELETE FROM permissions WHERE name = $1", name)
	return err
}

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
	Authenticate(ctx context.Context, credentials map[string]any) (*User, error)
	ValidateToken(token string) (*User, error)
}

// User represents an authenticated user
type User struct {
	ID          string         `json:"id"`
	Username    string         `json:"username"`
	Password    string         `json:"-"`
	Roles       []string       `json:"roles"`
	Permissions []string       `json:"permissions"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	LastLoginAt *time.Time     `json:"last_login_at,omitempty"`
}

// RoleManager manages user roles and permissions
type RoleManager struct {
	roleStorage       RoleStorage
	permissionStorage PermissionStorage
	mu                sync.RWMutex
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
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	EventType string         `json:"event_type"`
	UserID    string         `json:"user_id,omitempty"`
	Resource  string         `json:"resource"`
	Action    string         `json:"action"`
	IPAddress string         `json:"ip_address,omitempty"`
	UserAgent string         `json:"user_agent,omitempty"`
	Success   bool           `json:"success"`
	Details   map[string]any `json:"details,omitempty"`
}

// SessionManager manages user sessions
type SessionManager struct {
	sessions map[string]*Session
	maxAge   time.Duration
	mu       sync.RWMutex
}

// Session represents a user session
type Session struct {
	ID        string         `json:"id"`
	UserID    string         `json:"user_id"`
	CreatedAt time.Time      `json:"created_at"`
	ExpiresAt time.Time      `json:"expires_at"`
	IPAddress string         `json:"ip_address"`
	UserAgent string         `json:"user_agent"`
	Data      map[string]any `json:"data,omitempty"`
}

// NewSecurityManager creates a new security manager
func NewSecurityManager() *SecurityManager {
	key := make([]byte, 32)
	rand.Read(key)

	// Create memory storages by default
	userStorage := NewMemoryUserStorage()
	roleStorage := NewMemoryRoleStorage()
	permissionStorage := NewMemoryPermissionStorage()

	sm := &SecurityManager{
		authProviders:  make(map[string]AuthProvider),
		roleManager:    NewRoleManager(roleStorage, permissionStorage),
		rateLimiter:    NewSecurityRateLimiter(5, time.Minute*15), // 5 attempts per 15 minutes
		auditLogger:    NewAuditLogger(10000),
		sessionManager: NewSessionManager(time.Hour * 24), // 24 hour sessions
		encryptionKey:  key,
	}

	// Add default basic auth provider
	basicProvider := NewBasicAuthProvider(userStorage)
	sm.AddAuthProvider(basicProvider)

	return sm
}

// NewRoleManager creates a new role manager
func NewRoleManager(roleStorage RoleStorage, permissionStorage PermissionStorage) *RoleManager {
	return &RoleManager{
		roleStorage:       roleStorage,
		permissionStorage: permissionStorage,
	}
}

// AddPermission adds a permission to the role manager
func (rm *RoleManager) AddPermission(perm *Permission) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.permissionStorage.SavePermission(perm)
}

// AddRole adds a role to the role manager
func (rm *RoleManager) AddRole(role *Role) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.roleStorage.SaveRole(role)
}

// HasPermission checks if a user has a specific permission
func (rm *RoleManager) HasPermission(user *User, permission string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, roleName := range user.Roles {
		role, err := rm.roleStorage.GetRole(roleName)
		if err != nil {
			continue
		}
		for _, perm := range role.Permissions {
			if perm == permission {
				return true
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
		role, err := rm.roleStorage.GetRole(roleName)
		if err != nil {
			continue
		}
		for _, perm := range role.Permissions {
			permissions[perm] = true
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
		Data:      make(map[string]any),
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
func (sm *SecurityManager) Authenticate(ctx context.Context, credentials map[string]any) (*User, error) {
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
				Details: map[string]any{
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
		Details: map[string]any{
			"error": lastErr.Error(),
		},
	})

	return nil, fmt.Errorf("authentication failed: %w", lastErr)
}

func (sm *SecurityManager) AddPermission(perm *Permission) error {
	if perm == nil || (perm.Name == "" && (perm.Resource == "" || perm.Action == "")) {
		return fmt.Errorf("invalid permission")
	}
	return sm.roleManager.AddPermission(perm)
}

func (sm *SecurityManager) AddRole(role *Role) error {
	if role == nil || role.Name == "" {
		return fmt.Errorf("invalid role")
	}
	return sm.roleManager.AddRole(role)
}

func (sm *SecurityManager) AddUsers(users ...*User) error {
	for _, user := range users {
		if user == nil || user.Username == "" || user.Password == "" {
			return fmt.Errorf("invalid user")
		}
		if err := sm.AddUser(user); err != nil {
			return err
		}
	}
	return nil
}

func (sm *SecurityManager) AddRoles(roles ...*Role) error {
	for _, role := range roles {
		if err := sm.AddRole(role); err != nil {
			return err
		}
	}
	return nil
}

func (sm *SecurityManager) AddPermissions(perms ...*Permission) error {
	for _, perm := range perms {
		if err := sm.AddPermission(perm); err != nil {
			return err
		}
	}
	return nil
}

// AddUser adds a user to the system
func (sm *SecurityManager) AddUser(user *User) error {
	for _, provider := range sm.authProviders {
		if bap, ok := provider.(*BasicAuthProvider); ok {
			return bap.AddUser(user, user.Password)
		}
	}
	return fmt.Errorf("no suitable auth provider found")
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
			Details: map[string]any{
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
	userStorage UserStorage
}

func NewBasicAuthProvider(userStorage UserStorage) *BasicAuthProvider {
	return &BasicAuthProvider{
		userStorage: userStorage,
	}
}

func (bap *BasicAuthProvider) Name() string {
	return "basic"
}

func (bap *BasicAuthProvider) Authenticate(ctx context.Context, credentials map[string]any) (*User, error) {
	username, ok := credentials["username"].(string)
	if !ok {
		return nil, fmt.Errorf("username required")
	}

	password, ok := credentials["password"].(string)
	if !ok {
		return nil, fmt.Errorf("password required")
	}

	user, err := bap.userStorage.GetUser(username)
	if err != nil {
		return nil, fmt.Errorf("user not found")
	}

	storedPassword, err := bap.userStorage.GetPassword(username)
	if err != nil || storedPassword != password {
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
	return bap.Authenticate(context.Background(), map[string]any{
		"username": username,
		"password": "token", // Placeholder
	})
}

func (bap *BasicAuthProvider) AddUser(user *User, password string) error {
	// In production, hash the password
	user.CreatedAt = time.Now()
	return bap.userStorage.SaveUser(user, password)
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
func (sm *SecurityMiddleware) AuthenticateRequest(credentials map[string]any, ipAddress string) (*User, error) {
	user, err := sm.securityManager.Authenticate(context.Background(), credentials)
	if err != nil {
		// Log failed authentication attempt
		sm.securityManager.auditLogger.LogEvent(AuditEvent{
			EventType: "authentication",
			Action:    "login",
			Success:   false,
			Details: map[string]any{
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
