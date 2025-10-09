package pipeline

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Pipe is a middleware function that processes writer and input.
// It receives a next function that invokes the next pipe in the chain.
// The pipe can call next(ctx, writer, input) to continue the chain, or return early to short-circuit.
type Pipe[C context.Context, W, I any] func(ctx C, writer W, input I, next Next[C, W, I]) error
type Next[C context.Context, W, I any] func(C, W, I) error

// Pipeline chains multiple pipes together in a non-recursive middleware pattern.
type Pipeline[C context.Context, W, I any] struct {
	pipes    []Pipe[C, W, I]
	fittings []Next[C, W, I] // Pre-built next functions
}

func Done[C context.Context, W, I any]() Next[C, W, I] {
	return func(ctx C, writer W, input I) error {
		return nil
	}
}

// New creates a new pipeline with the given pipes.
// Pipes are executed in order: pipe1 -> pipe2 -> pipe3 -> ...
// Next functions are pre-built for efficiency.
func New[C context.Context, W, I any](ctx C, pipes ...Pipe[C, W, I]) *Pipeline[C, W, I] {
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

func (p *Pipeline[C, W, I]) Pipe(ctx C, writer W, input I, next Next[C, W, I]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.Process(ctx, writer, input); err != nil {
		return err
	}
	return next(ctx, writer, input)
}

// Process runs the pipeline with the given writer and input.
// Storage is automatically provided in the context for sharing data between pipes.
// If the context is cancelled, execution terminates and returns the context error.
func (p *Pipeline[C, W, I]) Process(ctx C, writer W, input I) error {
	if len(p.pipes) == 0 {
		return nil
	}

	// Check if context is already cancelled before starting
	if err := ctx.Err(); err != nil {
		return err
	}

	// Execute the first pipe with pre-built next functions
	err := p.pipes[0](ctx, writer, input, p.fittings[0])
	return err
}

// Deprecated: Use Process instead.
func (p *Pipeline[C, W, I]) Execute(ctx C, writer W, input I) error {
	return p.Process(ctx, writer, input)
}

// Adapter transforms the input before passing it to the next pipe.
// It acts like a TransformStream, applying the transform function to the input
// and passing the result down the pipeline chain.
// If the transform returns an error, the pipeline stops and the error is returned.
func Adapter[C context.Context, W, I any](transform func(C, I) (I, error)) Pipe[C, W, I] {
	return func(ctx C, writer W, input I, next Next[C, W, I]) error {
		// Apply transformation to the input
		transformed, err := transform(ctx, input)
		if err != nil {
			return err
		}

		// Pass transformed input to the next pipe
		return next(ctx, writer, transformed)
	}
}

// Wye splits the pipeline into two parallel branches.
// Both left and right pipes receive the same writer and input,
// execute in parallel, and both must complete before continuing.
// If either branch returns an error, execution stops and the error is returned.
func Wye[C context.Context, W, I any](left, right Pipe[C, W, I]) Pipe[C, W, I] {
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

// Diverter selects one or more pipes from multiple options based on a selector function.
// The selector function receives the context and input, and returns the indices of the pipes to execute.
// Selected pipes are executed in parallel, and all must complete before continuing.
// If the selector returns an empty slice, the chain continues immediately.
// If any selected pipe returns an error, execution stops and the error is returned.
func Diverter[C context.Context, W, I any](selector func(C, I) ([]int, error), pipes ...Pipe[C, W, I]) Pipe[C, W, I] {
	return func(ctx C, writer W, input I, next Next[C, W, I]) error {
		// Call selector to determine which pipes to use
		indices, err := selector(ctx, input)
		if err != nil {
			return err
		}

		// If no pipes selected, continue the chain
		if len(indices) == 0 {
			return next(ctx, writer, input)
		}

		// Validate all indices
		for _, idx := range indices {
			if idx < 0 || idx >= len(pipes) {
				return fmt.Errorf("invalid pipe index %d, must be between 0 and %d", idx, len(pipes)-1)
			}
		}

		// Execute selected pipes in parallel
		g, _ := errgroup.WithContext(ctx)
		for _, idx := range indices {
			idx := idx // Capture for closure
			g.Go(func() error {
				return pipes[idx](ctx, writer, input, func(C, W, I) error { return nil })
			})
		}

		// Wait for all selected pipes to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// All selected pipes succeeded, continue the chain
		return next(ctx, writer, input)
	}
}

// Joiner executes multiple pipes in parallel and waits for all to complete.
// All pipes receive the same writer and input, execute in parallel,
// and all must complete before continuing the chain.
// If any pipe returns an error, execution stops and the error is returned.
// This is a generalized version of Wye for N pipes.
func Joiner[C context.Context, W, I any](pipes ...Pipe[C, W, I]) Pipe[C, W, I] {
	return func(ctx C, writer W, input I, next Next[C, W, I]) error {
		// If no pipes provided, continue immediately
		if len(pipes) == 0 {
			return next(ctx, writer, input)
		}

		// Execute all pipes in parallel
		g, _ := errgroup.WithContext(ctx)
		for _, pipe := range pipes {
			pipe := pipe // Capture for closure
			g.Go(func() error {
				return pipe(ctx, writer, input, func(C, W, I) error { return nil })
			})
		}

		// Wait for all pipes to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// All pipes succeeded, continue the chain
		return next(ctx, writer, input)
	}
}
