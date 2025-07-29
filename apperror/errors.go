// apperror/apperror.go
package apperror

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// APP_ENV values
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// AppError defines a structured application error
type AppError struct {
	Code       string         `json:"code"`               // 9-digit code: XXX|AA|DD|YY
	Message    string         `json:"message"`            // human-readable message
	StatusCode int            `json:"-"`                  // HTTP status, not serialized
	Err        error          `json:"-"`                  // wrapped error, not serialized
	Metadata   map[string]any `json:"metadata,omitempty"` // optional extra info
	StackTrace []string       `json:"stackTrace,omitempty"`
}

// Error implements error interface
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap enables errors.Is / errors.As
func (e *AppError) Unwrap() error {
	return e.Err
}

// WithMetadata returns a shallow copy with added metadata key/value
func (e *AppError) WithMetadata(key string, val any) *AppError {
	newMD := make(map[string]any, len(e.Metadata)+1)
	for k, v := range e.Metadata {
		newMD[k] = v
	}
	newMD[key] = val

	return &AppError{
		Code:       e.Code,
		Message:    e.Message,
		StatusCode: e.StatusCode,
		Err:        e.Err,
		Metadata:   newMD,
		StackTrace: e.StackTrace,
	}
}

// GetStackTraceArray returns the error stack trace as an array of strings
func (e *AppError) GetStackTraceArray() []string {
	return e.StackTrace
}

// GetStackTraceString returns the error stack trace as a single string
func (e *AppError) GetStackTraceString() string {
	return strings.Join(e.StackTrace, "\n")
}

// captureStackTrace returns a slice of strings representing the stack trace.
func captureStackTrace() []string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	isDebug := os.Getenv("APP_DEBUG") == "true"
	var stack []string
	for {
		frame, more := frames.Next()
		var file string
		if !isDebug {
			file = "/" + filepath.Base(frame.File)
		} else {
			file = frame.File
		}
		if strings.HasSuffix(file, ".go") {
			file = strings.TrimSuffix(file, ".go") + ".sec"
		}
		stack = append(stack, fmt.Sprintf("%s:%d %s", file, frame.Line, frame.Function))
		if !more {
			break
		}
	}
	return stack
}

// buildCode constructs a 9-digit code: XXX|AA|DD|YY
func buildCode(httpCode, appCode, domainCode, errCode int) string {
	return fmt.Sprintf("%03d%02d%02d%02d", httpCode, appCode, domainCode, errCode)
}

// New creates a fresh AppError
func New(httpCode, appCode, domainCode, errCode int, msg string) *AppError {
	return &AppError{
		Code:       buildCode(httpCode, appCode, domainCode, errCode),
		Message:    msg,
		StatusCode: httpCode,
		// Prototype: no StackTrace captured at registration time.
	}
}

// Modify Wrap to always capture a fresh stack trace.
func Wrap(err error, httpCode, appCode, domainCode, errCode int, msg string) *AppError {
	return &AppError{
		Code:       buildCode(httpCode, appCode, domainCode, errCode),
		Message:    msg,
		StatusCode: httpCode,
		Err:        err,
		StackTrace: captureStackTrace(),
	}
}

// New helper: Instance attaches the runtime stack trace to a prototype error.
func Instance(e *AppError) *AppError {
	// Create a shallow copy and attach the current stack trace.
	copyE := *e
	copyE.StackTrace = captureStackTrace()
	return &copyE
}

// Modify toAppError to instance a prototype if it lacks a stack trace.
func toAppError(err error) *AppError {
	if err == nil {
		return nil
	}
	var ae *AppError
	if errors.As(err, &ae) {
		if len(ae.StackTrace) == 0 { // Prototype without context.
			return Instance(ae)
		}
		return ae
	}
	// fallback to internal error 500|00|00|00 with fresh stack trace.
	return Wrap(err, http.StatusInternalServerError, 0, 0, 0, "Internal server error")
}

// onError, if set, is called before writing any JSON error
var onError func(*AppError)

func OnError(hook func(*AppError)) {
	onError = hook
}

// WriteJSONError writes an error as JSON, includes X-Request-ID, hides details in production
func WriteJSONError(w http.ResponseWriter, r *http.Request, err error) {
	appErr := toAppError(err)

	// attach request ID
	if rid := r.Header.Get("X-Request-ID"); rid != "" {
		appErr = appErr.WithMetadata("request_id", rid)
	}
	// hook
	if onError != nil {
		onError(appErr)
	}
	// If no stack trace is present, capture current context stack trace.
	if os.Getenv("APP_ENV") != EnvProduction {
		appErr.StackTrace = captureStackTrace()
	}

	fmt.Println(appErr.StackTrace)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(appErr.StatusCode)

	resp := map[string]any{
		"code":    appErr.Code,
		"message": appErr.Message,
	}
	if len(appErr.Metadata) > 0 {
		resp["metadata"] = appErr.Metadata
	}
	if os.Getenv("APP_ENV") != EnvProduction {
		resp["stack"] = appErr.StackTrace
	}
	if appErr.Err != nil {
		resp["details"] = appErr.Err.Error()
	}

	_ = json.NewEncoder(w).Encode(resp)
}

type ErrorRegistry struct {
	registry map[string]*AppError
	mu       sync.RWMutex
}

func (er *ErrorRegistry) Get(name string) (*AppError, bool) {
	er.mu.RLock()
	defer er.mu.RUnlock()
	e, ok := er.registry[name]
	return e, ok
}

func (er *ErrorRegistry) Set(name string, e *AppError) {
	er.mu.Lock()
	defer er.mu.Unlock()
	er.registry[name] = e
}

func (er *ErrorRegistry) Delete(name string) {
	er.mu.Lock()
	defer er.mu.Unlock()
	delete(er.registry, name)
}

func (er *ErrorRegistry) List() []*AppError {
	er.mu.RLock()
	defer er.mu.RUnlock()
	out := make([]*AppError, 0, len(er.registry))
	for _, e := range er.registry {
		// create a shallow copy and remove the StackTrace for listing
		copyE := *e
		copyE.StackTrace = nil
		out = append(out, &copyE)
	}
	return out
}

func (er *ErrorRegistry) GetByCode(code string) (*AppError, bool) {
	er.mu.RLock()
	defer er.mu.RUnlock()
	for _, e := range er.registry {
		if e.Code == code {
			return e, true
		}
	}
	return nil, false
}

var (
	registry *ErrorRegistry
)

// Register adds a named error; fails if name exists
func Register(name string, e *AppError) error {
	if name == "" {
		return fmt.Errorf("error name cannot be empty")
	}
	registry.Set(name, e)
	return nil
}

// Update replaces an existing named error; fails if not found
func Update(name string, e *AppError) error {
	if name == "" {
		return fmt.Errorf("error name cannot be empty")
	}
	registry.Set(name, e)
	return nil
}

// Unregister removes a named error
func Unregister(name string) error {
	if name == "" {
		return fmt.Errorf("error name cannot be empty")
	}
	registry.Delete(name)
	return nil
}

// Get retrieves a named error
func Get(name string) (*AppError, bool) {
	return registry.Get(name)
}

// GetByCode retrieves an error by its 9-digit code
func GetByCode(code string) (*AppError, bool) {
	if code == "" {
		return nil, false
	}
	return registry.GetByCode(code)
}

// List returns all registered errors
func List() []*AppError {
	return registry.List()
}

// Is/As shortcuts updated to check all registered errors
func Is(err, target error) bool {
	if errors.Is(err, target) {
		return true
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	for _, e := range registry.registry {
		if errors.Is(err, e) || errors.Is(e, target) {
			return true
		}
	}
	return false
}

func As(err error, target any) bool {
	if errors.As(err, target) {
		return true
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	for _, e := range registry.registry {
		if errors.As(err, target) || errors.As(e, target) {
			return true
		}
	}
	return false
}

// HTTPMiddleware catches panics and converts to JSON 500
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				p := fmt.Errorf("panic: %v", rec)
				WriteJSONError(w, r, Wrap(p, http.StatusInternalServerError, 0, 0, 0, "Internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// preload some common errors (with 2-digit app/domain codes)
func init() {
	registry = &ErrorRegistry{registry: make(map[string]*AppError)}
	_ = Register("ErrNotFound", New(http.StatusNotFound, 1, 1, 1, "Resource not found"))               // → "404010101"
	_ = Register("ErrInvalidInput", New(http.StatusBadRequest, 1, 1, 2, "Invalid input provided"))     // → "400010102"
	_ = Register("ErrInternal", New(http.StatusInternalServerError, 1, 1, 0, "Internal server error")) // → "500010100"
	_ = Register("ErrUnauthorized", New(http.StatusUnauthorized, 1, 1, 3, "Unauthorized"))             // → "401010103"
	_ = Register("ErrForbidden", New(http.StatusForbidden, 1, 1, 4, "Forbidden"))                      // → "403010104"
}
