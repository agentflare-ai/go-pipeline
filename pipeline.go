package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

type key struct{}

var (
	ErrStorageNotFound = errors.New("storage not found")
	storagePool        = sync.Pool{
		New: func() any {
			return &sync.Map{}
		},
	}
	StorageKey = key{}
)

// Pipe is a middleware function that processes writer and input.
// It receives a next function that invokes the next pipe in the chain.
// The pipe can call next(ctx, writer, input) to continue the chain, or return early to short-circuit.
type Pipe[W, I any] func(ctx context.Context, writer W, input I, next func(context.Context, W, I) error) error

// Pipeline chains multiple pipes together in a non-recursive middleware pattern.
type Pipeline[W, I any] struct {
	baseCtx   context.Context
	pipes     []Pipe[W, I]
	nextFuncs []func(context.Context, W, I) error // Pre-built next functions
}

// New creates a new pipeline with the given pipes.
// Pipes are executed in order: pipe1 -> pipe2 -> pipe3 -> ...
// Next functions are pre-built for efficiency.
func New[W, I any](ctx context.Context, pipes ...Pipe[W, I]) *Pipeline[W, I] {
	p := &Pipeline[W, I]{
		baseCtx: ctx,
		pipes:   pipes,
	}

	if len(pipes) > 0 {
		p.buildNextFuncs()
	}

	return p
}

// buildNextFuncs pre-builds the next functions for the entire chain.
// This is called once during construction for maximum efficiency.
// Each next function checks for context cancellation before proceeding.
func (p *Pipeline[W, I]) buildNextFuncs() {
	p.nextFuncs = make([]func(context.Context, W, I) error, len(p.pipes))

	// Terminal next function (does nothing, chain complete)
	p.nextFuncs[len(p.pipes)-1] = func(ctx context.Context, writer W, input I) error {
		return nil
	}

	// Build next functions from end to start
	for i := len(p.pipes) - 2; i >= 0; i-- {
		// Capture variables for closure
		nextIdx := i + 1
		p.nextFuncs[i] = func(ctx context.Context, writer W, input I) error {
			// Check if context is cancelled before proceeding
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.pipes[nextIdx](ctx, writer, input, p.nextFuncs[nextIdx])
		}
	}
}

// Execute runs the pipeline with the given writer and input.
// Storage is automatically provided in the context for sharing data between pipes.
// If the context is cancelled, execution terminates and returns the context error.
func (p *Pipeline[W, I]) Execute(writer W, input I) error {
	if len(p.pipes) == 0 {
		return nil
	}

	// Check if context is already cancelled before starting
	if err := p.baseCtx.Err(); err != nil {
		slog.Error("pipeline context cancelled", "error", err)
		return err
	}

	storage := storagePool.Get().(*sync.Map)
	defer func() {
		// Clear storage before returning to pool
		storage.Clear()
		storagePool.Put(storage)
	}()
	ctx := context.WithValue(p.baseCtx, StorageKey, storage)

	// Execute the first pipe with pre-built next functions
	err := p.pipes[0](ctx, writer, input, p.nextFuncs[0])
	if err != nil {
		slog.Error("pipeline execution failed", "error", err)
	}
	return err
}

// Load retrieves a value from the pipeline storage.
// Returns the value and true if found, zero value and false otherwise.
func Load[T any](ctx context.Context, key any) (T, bool) {
	storage, ok := ctx.Value(StorageKey).(*sync.Map)
	if !ok {
		var v T
		return v, false
	}
	v, ok := storage.Load(key)
	if !ok {
		var v T
		return v, false
	}
	return v.(T), ok
}

// Store saves a value to the pipeline storage.
// Returns ErrStorageNotFound if storage is not available in the context.
func Store[T any](ctx context.Context, key any, value T) error {
	storage, ok := ctx.Value(StorageKey).(*sync.Map)
	if !ok {
		return ErrStorageNotFound
	}
	storage.Store(key, value)
	return nil
}
