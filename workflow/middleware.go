package workflow

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// MiddlewareManager manages middleware execution chain
type MiddlewareManager struct {
	middlewares []Middleware
	cache       map[string]*MiddlewareResult
	mutex       sync.RWMutex
}

// MiddlewareFunc is the function signature for middleware
type MiddlewareFunc func(ctx context.Context, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult

// MiddlewareChain represents a chain of middleware functions
type MiddlewareChain struct {
	middlewares []MiddlewareFunc
}

// NewMiddlewareManager creates a new middleware manager
func NewMiddlewareManager() *MiddlewareManager {
	return &MiddlewareManager{
		middlewares: make([]Middleware, 0),
		cache:       make(map[string]*MiddlewareResult),
	}
}

// AddMiddleware adds a middleware to the chain
func (m *MiddlewareManager) AddMiddleware(middleware Middleware) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Insert middleware in priority order
	inserted := false
	for i, existing := range m.middlewares {
		if middleware.Priority < existing.Priority {
			m.middlewares = append(m.middlewares[:i], append([]Middleware{middleware}, m.middlewares[i:]...)...)
			inserted = true
			break
		}
	}

	if !inserted {
		m.middlewares = append(m.middlewares, middleware)
	}
}

// Execute runs the middleware chain
func (m *MiddlewareManager) Execute(ctx context.Context, data map[string]interface{}) MiddlewareResult {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if len(m.middlewares) == 0 {
		return MiddlewareResult{Continue: true, Data: data}
	}

	return m.executeChain(ctx, data, 0)
}

// executeChain recursively executes middleware chain
func (m *MiddlewareManager) executeChain(ctx context.Context, data map[string]interface{}, index int) MiddlewareResult {
	if index >= len(m.middlewares) {
		return MiddlewareResult{Continue: true, Data: data}
	}

	middleware := m.middlewares[index]
	if !middleware.Enabled {
		return m.executeChain(ctx, data, index+1)
	}

	// Create the next function
	next := func(ctx context.Context, data map[string]interface{}) MiddlewareResult {
		return m.executeChain(ctx, data, index+1)
	}

	// Execute current middleware
	return m.executeMiddleware(ctx, middleware, data, next)
}

// executeMiddleware executes a single middleware
func (m *MiddlewareManager) executeMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	switch middleware.Type {
	case MiddlewareAuth:
		return m.executeAuthMiddleware(ctx, middleware, data, next)
	case MiddlewareLogging:
		return m.executeLoggingMiddleware(ctx, middleware, data, next)
	case MiddlewareRateLimit:
		return m.executeRateLimitMiddleware(ctx, middleware, data, next)
	case MiddlewareValidate:
		return m.executeValidateMiddleware(ctx, middleware, data, next)
	case MiddlewareTransform:
		return m.executeTransformMiddleware(ctx, middleware, data, next)
	case MiddlewareCustom:
		return m.executeCustomMiddleware(ctx, middleware, data, next)
	default:
		// Unknown middleware type, continue
		return next(ctx, data)
	}
}

// Auth middleware implementation
func (m *MiddlewareManager) executeAuthMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	// Extract token from data or context
	token, exists := data["auth_token"].(string)
	if !exists {
		if authHeader, ok := data["headers"].(map[string]string); ok {
			if auth, ok := authHeader["Authorization"]; ok {
				token = auth
			}
		}
	}

	if token == "" {
		return MiddlewareResult{
			Continue: false,
			Error:    fmt.Errorf("authentication token required"),
			Data:     data,
		}
	}

	// Validate token (simplified)
	if !isValidToken(token) {
		return MiddlewareResult{
			Continue: false,
			Error:    fmt.Errorf("invalid authentication token"),
			Data:     data,
		}
	}

	// Add user context
	username := extractUsernameFromToken(token)
	user := &User{
		ID:          username,
		Username:    username,
		Role:        UserRoleOperator,
		Permissions: getUserPermissions(username),
	}

	authContext := &AuthContext{
		User:        user,
		Token:       token,
		Permissions: user.Permissions,
	}

	data["auth_context"] = authContext
	data["user"] = user

	return next(ctx, data)
}

// Logging middleware implementation
func (m *MiddlewareManager) executeLoggingMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	startTime := time.Now()

	// Log request
	log.Printf("[MIDDLEWARE] %s - Started processing request", middleware.Name)

	// Continue to next middleware
	result := next(ctx, data)

	// Log response
	duration := time.Since(startTime)
	if result.Error != nil {
		log.Printf("[MIDDLEWARE] %s - Completed with error in %v: %v", middleware.Name, duration, result.Error)
	} else {
		log.Printf("[MIDDLEWARE] %s - Completed successfully in %v", middleware.Name, duration)
	}

	return result
}

// Rate limiting middleware implementation
func (m *MiddlewareManager) executeRateLimitMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	// Get user/IP for rate limiting
	identifier := "anonymous"
	if user, exists := data["user"].(*User); exists {
		identifier = user.ID
	} else if ip, exists := data["client_ip"].(string); exists {
		identifier = ip
	}

	// Check rate limit (simplified implementation)
	limit := getConfigInt(middleware.Config, "requests_per_minute", 60)
	if !checkRateLimit(identifier, limit) {
		return MiddlewareResult{
			Continue: false,
			Error:    fmt.Errorf("rate limit exceeded for %s", identifier),
			Data:     data,
		}
	}

	return next(ctx, data)
}

// Validation middleware implementation
func (m *MiddlewareManager) executeValidateMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	// Get validation rules from config
	rules, exists := middleware.Config["rules"].([]interface{})
	if !exists {
		return next(ctx, data)
	}

	// Validate data
	for _, rule := range rules {
		if ruleMap, ok := rule.(map[string]interface{}); ok {
			field := ruleMap["field"].(string)
			ruleType := ruleMap["type"].(string)

			if err := validateDataField(data, field, ruleType, ruleMap); err != nil {
				return MiddlewareResult{
					Continue: false,
					Error:    fmt.Errorf("validation failed: %v", err),
					Data:     data,
				}
			}
		}
	}

	return next(ctx, data)
}

// Transform middleware implementation
func (m *MiddlewareManager) executeTransformMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	// Get transformation rules from config
	transforms, exists := middleware.Config["transforms"].(map[string]interface{})
	if !exists {
		return next(ctx, data)
	}

	// Apply transformations
	for field, transform := range transforms {
		if transformType, ok := transform.(string); ok {
			switch transformType {
			case "lowercase":
				if value, exists := data[field].(string); exists {
					data[field] = strings.ToLower(value)
				}
			case "uppercase":
				if value, exists := data[field].(string); exists {
					data[field] = strings.ToUpper(value)
				}
			case "trim":
				if value, exists := data[field].(string); exists {
					data[field] = strings.TrimSpace(value)
				}
			}
		}
	}

	return next(ctx, data)
}

// Custom middleware implementation
func (m *MiddlewareManager) executeCustomMiddleware(ctx context.Context, middleware Middleware, data map[string]interface{}, next func(context.Context, map[string]interface{}) MiddlewareResult) MiddlewareResult {
	// Custom middleware can be implemented by users
	// For now, just pass through
	return next(ctx, data)
}

// Permission checking
type PermissionChecker struct {
	permissions map[string][]Permission
	mutex       sync.RWMutex
}

// NewPermissionChecker creates a new permission checker
func NewPermissionChecker() *PermissionChecker {
	return &PermissionChecker{
		permissions: make(map[string][]Permission),
	}
}

// AddPermission adds a permission for a user
func (p *PermissionChecker) AddPermission(userID string, permission Permission) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.permissions[userID] == nil {
		p.permissions[userID] = make([]Permission, 0)
	}

	p.permissions[userID] = append(p.permissions[userID], permission)
}

// CheckPermission checks if a user has permission for an action
func (p *PermissionChecker) CheckPermission(userID, resource string, action PermissionAction) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	permissions, exists := p.permissions[userID]
	if !exists {
		return false
	}

	for _, perm := range permissions {
		if perm.Resource == resource && perm.Action == action {
			return true
		}
		// Check for admin permission
		if perm.Action == PermissionAdmin {
			return true
		}
	}

	return false
}

// Utility functions for middleware

// Rate limiting cache
var rateLimitCache = make(map[string][]time.Time)
var rateLimitMutex sync.RWMutex

func checkRateLimit(identifier string, requestsPerMinute int) bool {
	rateLimitMutex.Lock()
	defer rateLimitMutex.Unlock()

	now := time.Now()
	cutoff := now.Add(-time.Minute)

	// Initialize if not exists
	if rateLimitCache[identifier] == nil {
		rateLimitCache[identifier] = make([]time.Time, 0)
	}

	// Remove old entries
	requests := rateLimitCache[identifier]
	validRequests := make([]time.Time, 0)
	for _, req := range requests {
		if req.After(cutoff) {
			validRequests = append(validRequests, req)
		}
	}

	// Check if limit exceeded
	if len(validRequests) >= requestsPerMinute {
		return false
	}

	// Add current request
	validRequests = append(validRequests, now)
	rateLimitCache[identifier] = validRequests

	return true
}

func getConfigInt(config map[string]interface{}, key string, defaultValue int) int {
	if value, exists := config[key]; exists {
		if intValue, ok := value.(int); ok {
			return intValue
		}
		if floatValue, ok := value.(float64); ok {
			return int(floatValue)
		}
	}
	return defaultValue
}

func validateDataField(data map[string]interface{}, field, ruleType string, rule map[string]interface{}) error {
	value, exists := data[field]

	switch ruleType {
	case "required":
		if !exists || value == nil || value == "" {
			return fmt.Errorf("field '%s' is required", field)
		}
	case "type":
		expectedType := rule["expected"].(string)
		if !isCorrectType(value, expectedType) {
			return fmt.Errorf("field '%s' must be of type %s", field, expectedType)
		}
	case "length":
		if str, ok := value.(string); ok {
			minLen := int(rule["min"].(float64))
			maxLen := int(rule["max"].(float64))
			if len(str) < minLen || len(str) > maxLen {
				return fmt.Errorf("field '%s' length must be between %d and %d", field, minLen, maxLen)
			}
		}
	}

	return nil
}

// User management system
type UserManager struct {
	users             map[string]*User
	sessions          map[string]*AuthContext
	permissionChecker *PermissionChecker
	mutex             sync.RWMutex
}

// NewUserManager creates a new user manager
func NewUserManager() *UserManager {
	return &UserManager{
		users:             make(map[string]*User),
		sessions:          make(map[string]*AuthContext),
		permissionChecker: NewPermissionChecker(),
	}
}

// CreateUser creates a new user
func (u *UserManager) CreateUser(user *User) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	if _, exists := u.users[user.ID]; exists {
		return fmt.Errorf("user %s already exists", user.ID)
	}

	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	u.users[user.ID] = user

	// Add default permissions based on role
	u.addDefaultPermissions(user)

	return nil
}

// GetUser retrieves a user by ID
func (u *UserManager) GetUser(userID string) (*User, error) {
	u.mutex.RLock()
	defer u.mutex.RUnlock()

	user, exists := u.users[userID]
	if !exists {
		return nil, fmt.Errorf("user %s not found", userID)
	}

	return user, nil
}

// AuthenticateUser authenticates a user and creates a session
func (u *UserManager) AuthenticateUser(username, password string) (*AuthContext, error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Find user by username
	var user *User
	for _, u := range u.users {
		if u.Username == username {
			user = u
			break
		}
	}

	if user == nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	// In production, properly hash and verify password
	if password != "password" {
		return nil, fmt.Errorf("invalid credentials")
	}

	// Create session
	sessionID := generateSessionID()
	token := generateToken(user)

	authContext := &AuthContext{
		User:        user,
		SessionID:   sessionID,
		Token:       token,
		Permissions: user.Permissions,
	}

	u.sessions[sessionID] = authContext

	return authContext, nil
}

// ValidateSession validates a session token
func (u *UserManager) ValidateSession(token string) (*AuthContext, error) {
	u.mutex.RLock()
	defer u.mutex.RUnlock()

	for _, session := range u.sessions {
		if session.Token == token {
			return session, nil
		}
	}

	return nil, fmt.Errorf("invalid session token")
}

// addDefaultPermissions adds default permissions based on user role
func (u *UserManager) addDefaultPermissions(user *User) {
	switch user.Role {
	case UserRoleAdmin:
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "*",
			Action:   PermissionAdmin,
		})
	case UserRoleManager:
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionRead,
		})
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionWrite,
		})
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionExecute,
		})
	case UserRoleOperator:
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionRead,
		})
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionExecute,
		})
	case UserRoleViewer:
		u.permissionChecker.AddPermission(user.ID, Permission{
			Resource: "workflow",
			Action:   PermissionRead,
		})
	}
}

func generateSessionID() string {
	return fmt.Sprintf("session_%d", time.Now().UnixNano())
}

// Helper functions for authentication middleware
func isValidToken(token string) bool {
	// Simple token validation - in real implementation, verify JWT or session token
	return token != "" && len(token) > 10
}

func extractUsernameFromToken(token string) string {
	// Simple username extraction - in real implementation, decode JWT claims
	if strings.HasPrefix(token, "bearer_") {
		return strings.TrimPrefix(token, "bearer_")
	}
	return "unknown"
}

func getUserPermissions(username string) []string {
	// Simple permission mapping - in real implementation, fetch from database
	switch username {
	case "admin":
		return []string{"read", "write", "execute", "delete"}
	case "manager":
		return []string{"read", "write", "execute"}
	default:
		return []string{"read"}
	}
}

func isCorrectType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		if !ok {
			_, ok = value.(int)
		}
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]interface{})
		return ok
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	default:
		return false
	}
}

func generateToken(user *User) string {
	// Simple token generation - in real implementation, create JWT
	return fmt.Sprintf("token_%s_%d", user.Username, time.Now().Unix())
}
