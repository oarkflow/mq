package workflow

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// InMemoryRegistry - In-memory implementation of WorkflowRegistry
type InMemoryRegistry struct {
	workflows map[string]*WorkflowDefinition
	versions  map[string][]string // workflow_id -> list of versions
	mu        sync.RWMutex
}

// NewInMemoryRegistry creates a new in-memory workflow registry
func NewInMemoryRegistry() WorkflowRegistry {
	return &InMemoryRegistry{
		workflows: make(map[string]*WorkflowDefinition),
		versions:  make(map[string][]string),
	}
}

func (r *InMemoryRegistry) Store(ctx context.Context, definition *WorkflowDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create a unique key for this version
	key := fmt.Sprintf("%s:%s", definition.ID, definition.Version)

	// Store the workflow
	r.workflows[key] = definition

	// Track versions
	if versions, exists := r.versions[definition.ID]; exists {
		// Check if version already exists
		found := false
		for _, v := range versions {
			if v == definition.Version {
				found = true
				break
			}
		}
		if !found {
			r.versions[definition.ID] = append(versions, definition.Version)
		}
	} else {
		r.versions[definition.ID] = []string{definition.Version}
	}

	return nil
}

func (r *InMemoryRegistry) Get(ctx context.Context, id string, version string) (*WorkflowDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var key string
	if version == "" {
		// Get latest version
		versions, exists := r.versions[id]
		if !exists || len(versions) == 0 {
			return nil, fmt.Errorf("workflow not found: %s", id)
		}

		// Sort versions and get the latest
		sort.Slice(versions, func(i, j int) bool {
			return versions[i] > versions[j] // Assuming version strings are sortable
		})
		key = fmt.Sprintf("%s:%s", id, versions[0])
	} else {
		key = fmt.Sprintf("%s:%s", id, version)
	}

	definition, exists := r.workflows[key]
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s (version: %s)", id, version)
	}

	return definition, nil
}

func (r *InMemoryRegistry) List(ctx context.Context, filter *WorkflowFilter) ([]*WorkflowDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []*WorkflowDefinition

	for _, definition := range r.workflows {
		if r.matchesFilter(definition, filter) {
			results = append(results, definition)
		}
	}

	// Apply sorting
	if filter != nil && filter.SortBy != "" {
		r.sortResults(results, filter.SortBy, filter.SortOrder)
	}

	// Apply pagination
	if filter != nil {
		start := filter.Offset
		end := start + filter.Limit

		if start >= len(results) {
			return []*WorkflowDefinition{}, nil
		}

		if end > len(results) {
			end = len(results)
		}

		if filter.Limit > 0 {
			results = results[start:end]
		}
	}

	return results, nil
}

func (r *InMemoryRegistry) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Get all versions for this workflow
	versions, exists := r.versions[id]
	if !exists {
		return fmt.Errorf("workflow not found: %s", id)
	}

	// Delete all versions
	for _, version := range versions {
		key := fmt.Sprintf("%s:%s", id, version)
		delete(r.workflows, key)
	}

	// Remove from versions map
	delete(r.versions, id)

	return nil
}

func (r *InMemoryRegistry) GetVersions(ctx context.Context, id string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.versions[id]
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}

	// Return a copy to avoid modification
	result := make([]string, len(versions))
	copy(result, versions)

	// Sort versions
	sort.Slice(result, func(i, j int) bool {
		return result[i] > result[j]
	})

	return result, nil
}

func (r *InMemoryRegistry) matchesFilter(definition *WorkflowDefinition, filter *WorkflowFilter) bool {
	if filter == nil {
		return true
	}

	// Filter by status
	if len(filter.Status) > 0 {
		found := false
		for _, status := range filter.Status {
			if definition.Status == status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by category
	if len(filter.Category) > 0 {
		found := false
		for _, category := range filter.Category {
			if definition.Category == category {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by owner
	if len(filter.Owner) > 0 {
		found := false
		for _, owner := range filter.Owner {
			if definition.Owner == owner {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by tags
	if len(filter.Tags) > 0 {
		for _, filterTag := range filter.Tags {
			found := false
			for _, defTag := range definition.Tags {
				if defTag == filterTag {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}

	// Filter by creation date
	if filter.CreatedFrom != nil && definition.CreatedAt.Before(*filter.CreatedFrom) {
		return false
	}

	if filter.CreatedTo != nil && definition.CreatedAt.After(*filter.CreatedTo) {
		return false
	}

	// Filter by search term
	if filter.Search != "" {
		searchTerm := strings.ToLower(filter.Search)
		if !strings.Contains(strings.ToLower(definition.Name), searchTerm) &&
			!strings.Contains(strings.ToLower(definition.Description), searchTerm) {
			return false
		}
	}

	return true
}

func (r *InMemoryRegistry) sortResults(results []*WorkflowDefinition, sortBy, sortOrder string) {
	ascending := sortOrder != "desc"

	switch sortBy {
	case "name":
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].Name < results[j].Name
			}
			return results[i].Name > results[j].Name
		})
	case "created_at":
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].CreatedAt.Before(results[j].CreatedAt)
			}
			return results[i].CreatedAt.After(results[j].CreatedAt)
		})
	case "updated_at":
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].UpdatedAt.Before(results[j].UpdatedAt)
			}
			return results[i].UpdatedAt.After(results[j].UpdatedAt)
		})
	default:
		// Default sort by name
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].Name < results[j].Name
			}
			return results[i].Name > results[j].Name
		})
	}
}

// InMemoryStateManager - In-memory implementation of StateManager
type InMemoryStateManager struct {
	executions  map[string]*Execution
	checkpoints map[string][]*Checkpoint // execution_id -> checkpoints
	mu          sync.RWMutex
}

// NewInMemoryStateManager creates a new in-memory state manager
func NewInMemoryStateManager() StateManager {
	return &InMemoryStateManager{
		executions:  make(map[string]*Execution),
		checkpoints: make(map[string][]*Checkpoint),
	}
}

func (s *InMemoryStateManager) CreateExecution(ctx context.Context, execution *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if execution.ID == "" {
		return fmt.Errorf("execution ID cannot be empty")
	}

	s.executions[execution.ID] = execution
	return nil
}

func (s *InMemoryStateManager) UpdateExecution(ctx context.Context, execution *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.executions[execution.ID]; !exists {
		return fmt.Errorf("execution not found: %s", execution.ID)
	}

	execution.UpdatedAt = time.Now()
	s.executions[execution.ID] = execution
	return nil
}

func (s *InMemoryStateManager) GetExecution(ctx context.Context, executionID string) (*Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	execution, exists := s.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution not found: %s", executionID)
	}

	return execution, nil
}

func (s *InMemoryStateManager) ListExecutions(ctx context.Context, filter *ExecutionFilter) ([]*Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*Execution

	for _, execution := range s.executions {
		if s.matchesExecutionFilter(execution, filter) {
			results = append(results, execution)
		}
	}

	// Apply sorting
	if filter != nil && filter.SortBy != "" {
		s.sortExecutionResults(results, filter.SortBy, filter.SortOrder)
	}

	// Apply pagination
	if filter != nil {
		start := filter.Offset
		end := start + filter.Limit

		if start >= len(results) {
			return []*Execution{}, nil
		}

		if end > len(results) {
			end = len(results)
		}

		if filter.Limit > 0 {
			results = results[start:end]
		}
	}

	return results, nil
}

func (s *InMemoryStateManager) DeleteExecution(ctx context.Context, executionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.executions, executionID)
	delete(s.checkpoints, executionID)
	return nil
}

func (s *InMemoryStateManager) SaveCheckpoint(ctx context.Context, executionID string, checkpoint *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if checkpoints, exists := s.checkpoints[executionID]; exists {
		s.checkpoints[executionID] = append(checkpoints, checkpoint)
	} else {
		s.checkpoints[executionID] = []*Checkpoint{checkpoint}
	}

	return nil
}

func (s *InMemoryStateManager) GetCheckpoints(ctx context.Context, executionID string) ([]*Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	checkpoints, exists := s.checkpoints[executionID]
	if !exists {
		return []*Checkpoint{}, nil
	}

	// Return a copy
	result := make([]*Checkpoint, len(checkpoints))
	copy(result, checkpoints)

	return result, nil
}

func (s *InMemoryStateManager) matchesExecutionFilter(execution *Execution, filter *ExecutionFilter) bool {
	if filter == nil {
		return true
	}

	// Filter by workflow ID
	if len(filter.WorkflowID) > 0 {
		found := false
		for _, workflowID := range filter.WorkflowID {
			if execution.WorkflowID == workflowID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by status
	if len(filter.Status) > 0 {
		found := false
		for _, status := range filter.Status {
			if execution.Status == status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by owner
	if len(filter.Owner) > 0 {
		found := false
		for _, owner := range filter.Owner {
			if execution.Owner == owner {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by priority
	if len(filter.Priority) > 0 {
		found := false
		for _, priority := range filter.Priority {
			if execution.Priority == priority {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by start date
	if filter.StartedFrom != nil && execution.StartedAt.Before(*filter.StartedFrom) {
		return false
	}

	if filter.StartedTo != nil && execution.StartedAt.After(*filter.StartedTo) {
		return false
	}

	return true
}

func (s *InMemoryStateManager) sortExecutionResults(results []*Execution, sortBy, sortOrder string) {
	ascending := sortOrder != "desc"

	switch sortBy {
	case "started_at":
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].StartedAt.Before(results[j].StartedAt)
			}
			return results[i].StartedAt.After(results[j].StartedAt)
		})
	case "updated_at":
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].UpdatedAt.Before(results[j].UpdatedAt)
			}
			return results[i].UpdatedAt.After(results[j].UpdatedAt)
		})
	case "priority":
		sort.Slice(results, func(i, j int) bool {
			priorityOrder := map[Priority]int{
				PriorityLow:      1,
				PriorityMedium:   2,
				PriorityHigh:     3,
				PriorityCritical: 4,
			}

			pi := priorityOrder[results[i].Priority]
			pj := priorityOrder[results[j].Priority]

			if ascending {
				return pi < pj
			}
			return pi > pj
		})
	default:
		// Default sort by started_at
		sort.Slice(results, func(i, j int) bool {
			if ascending {
				return results[i].StartedAt.Before(results[j].StartedAt)
			}
			return results[i].StartedAt.After(results[j].StartedAt)
		})
	}
}
