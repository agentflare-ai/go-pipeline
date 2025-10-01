package pipeline

import (
	"context"
	"log/slog"
)

// Pipe is a middleware function that processes writer and input.
// It receives a next function that invokes the next pipe in the chain.
// The pipe can call next(ctx, writer, input) to continue the chain, or return early to short-circuit.
type Pipe[C context.Context, W, I any] func(ctx C, writer W, input I, next func(C, W, I) error) error

// Pipeline chains multiple pipes together in a non-recursive middleware pattern.
type Pipeline[C context.Context, W, I any] struct {
	ctx       C
	pipes     []Pipe[C, W, I]
	nextFuncs []func(C, W, I) error // Pre-built next functions
}

// New creates a new pipeline with the given pipes.
// Pipes are executed in order: pipe1 -> pipe2 -> pipe3 -> ...
// Next functions are pre-built for efficiency.
func New[C context.Context, W, I any](ctx C, pipes ...Pipe[C, W, I]) *Pipeline[C, W, I] {
	p := &Pipeline[C, W, I]{
		ctx:   ctx,
		pipes: pipes,
	}

	if len(pipes) > 0 {
		p.buildNextFuncs()
	}

	return p
}

func (p *Pipeline[C, W, I]) Context() C {
	return p.ctx
}

// buildNextFuncs pre-builds the next functions for the entire chain.
// This is called once during construction for maximum efficiency.
// Each next function checks for context cancellation before proceeding.
func (p *Pipeline[C, W, I]) buildNextFuncs() {
	p.nextFuncs = make([]func(C, W, I) error, len(p.pipes))

	// Terminal next function (does nothing, chain complete)
	p.nextFuncs[len(p.pipes)-1] = func(ctx C, writer W, input I) error {
		return nil
	}

	// Build next functions from end to start
	for i := len(p.pipes) - 2; i >= 0; i-- {
		// Capture variables for closure
		nextIdx := i + 1
		p.nextFuncs[i] = func(ctx C, writer W, input I) error {
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
func (p *Pipeline[C, W, I]) Execute(writer W, input I) error {
	if len(p.pipes) == 0 {
		return nil
	}

	// Check if context is already cancelled before starting
	if err := p.ctx.Err(); err != nil {
		slog.Error("pipeline context cancelled", "error", err)
		return err
	}

	// Execute the first pipe with pre-built next functions
	err := p.pipes[0](p.ctx, writer, input, p.nextFuncs[0])
	if err != nil {
		slog.Error("pipeline execution failed", "error", err)
	}
	return err
}
