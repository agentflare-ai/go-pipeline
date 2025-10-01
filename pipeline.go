package pipeline

import (
	"context"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

// Fitting is a middleware function that processes writer and input.
// It receives a next function that invokes the next pipe in the chain.
// The pipe can call next(ctx, writer, input) to continue the chain, or return early to short-circuit.
type Fitting[C context.Context, W, I any] func(ctx C, writer W, input I, next Next[C, W, I]) error
type Next[C context.Context, W, I any] func(C, W, I) error

// Pipeline chains multiple pipes together in a non-recursive middleware pattern.
type Pipeline[C context.Context, W, I any] struct {
	pipes    []Fitting[C, W, I]
	fittings []Next[C, W, I] // Pre-built next functions
}

// New creates a new pipeline with the given pipes.
// Pipes are executed in order: pipe1 -> pipe2 -> pipe3 -> ...
// Next functions are pre-built for efficiency.
func New[C context.Context, W, I any](ctx C, pipes ...Fitting[C, W, I]) *Pipeline[C, W, I] {
	p := &Pipeline[C, W, I]{
		pipes: pipes,
	}

	if len(pipes) > 0 {
		p.connect()
	}

	return p
}

// connect pre-builds the next functions for the entire chain.
// This is called once during construction for maximum efficiency.
// Each next function checks for context cancellation before proceeding.
func (p *Pipeline[C, W, I]) connect() {
	p.fittings = make([]Next[C, W, I], len(p.pipes))

	// Terminal next function (does nothing, chain complete)
	p.fittings[len(p.pipes)-1] = func(ctx C, writer W, input I) error {
		return nil
	}

	// Build next functions from end to start
	for i := len(p.pipes) - 2; i >= 0; i-- {
		// Capture variables for closure
		nextIdx := i + 1
		p.fittings[i] = func(ctx C, writer W, input I) error {
			// Check if context is cancelled before proceeding
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.pipes[nextIdx](ctx, writer, input, p.fittings[nextIdx])
		}
	}
}

// Execute runs the pipeline with the given writer and input.
// Storage is automatically provided in the context for sharing data between pipes.
// If the context is cancelled, execution terminates and returns the context error.
func (p *Pipeline[C, W, I]) Execute(ctx C, writer W, input I) error {
	if len(p.pipes) == 0 {
		return nil
	}

	// Check if context is already cancelled before starting
	if err := ctx.Err(); err != nil {
		slog.Error("pipeline context cancelled", "error", err)
		return err
	}

	// Execute the first pipe with pre-built next functions
	err := p.pipes[0](ctx, writer, input, p.fittings[0])
	if err != nil {
		slog.Error("pipeline execution failed", "error", err)
	}
	return err
}

// Wye splits the pipeline into two parallel branches.
// Both left and right pipes receive the same writer and input,
// execute in parallel, and both must complete before continuing.
// If either branch returns an error, execution stops and the error is returned.
func Wye[C context.Context, W, I any](left, right Fitting[C, W, I]) Fitting[C, W, I] {
	return func(ctx C, writer W, input I, next Next[C, W, I]) error {
		g, _ := errgroup.WithContext(ctx)

		g.Go(func() error {
			return left(ctx, writer, input, func(C, W, I) error { return nil })
		})

		g.Go(func() error {
			return right(ctx, writer, input, func(C, W, I) error { return nil })
		})

		// Wait for both to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// Both branches succeeded, continue the chain
		return next(ctx, writer, input)
	}
}
