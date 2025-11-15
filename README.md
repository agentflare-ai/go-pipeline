## Pipeline

A generic, type-safe middleware pipeline implementation for Go 1.24+ with context support and parallel composition helpers.

## Overview

Pipeline provides a flexible middleware pattern for chaining processing functions together. It uses Go generics to provide type-safe operations on any sink and input types, with built-in context cancellation.

## Features

* **Generic Types**: Type-safe pipeline with support for any context (`C`), sink (`O`), and input (`I`) types
* **Pipeline Composition**: Pipelines can be used as pipes in other pipelines via the `Pipe` method
* **Context Cancellation**: Full support for context-based cancellation at any pipeline stage
* **Non-Recursive**: Pre-built next functions eliminate stack depth concerns
* **Short-Circuiting**: Pipes can skip downstream processing by not calling `next()`
* **Error Propagation**: Errors bubble up through the chain automatically

## Installation

```bash
go get github.com/agentflare-ai/go-pipeline
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    pipeline "github.com/agentflare-ai/go-pipeline"
)

type Writer struct {
    data []string
}

func (w *Writer) Push(s string) (int, error) {
    w.data = append(w.data, s)
    return len(s), nil
}

func (w *Writer) Flush() error {
    return nil
}

func (w *Writer) Close() error {
    return nil
}

func main() {
    ctx := context.Background()

    // Create pipeline with logging and processing pipes
    p := pipeline.New(ctx,
        // Logging pipe
        func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
            fmt.Println("Processing:", input)
            return next(ctx, sink, input)
        },
        // Processing pipe
        func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
            sink.Push("Processed: " + input)
            return next(ctx, sink, input)
        },
    )

    writer := &Writer{}
    if err := p.Process(ctx, writer, "hello world"); err != nil {
        panic(err)
    }

    fmt.Println(writer.data) // ["Processed: hello world"]
}
```

## Core Concepts

### Pipe Function

A `Pipe` is a middleware function with the signature:

```go
type Pipe[C context.Context, O, I any] func(ctx C, sink O, source I, next NextPipe[C, O, I]) error
```

Each pipe receives:

* `ctx`: Context for cancellation (generic type `C` must satisfy `context.Context`)
* `sink`: The sink instance to operate on (generic type `O`)
* `source`: The input data to process (generic type `I`)
* `next`: Function to invoke the next pipe in the chain

### PipeAdapter

The `PipeAdapter` helper creates a pipe that transforms input data, similar to a TransformStream:

```go
func PipeAdapter[C context.Context, O, I any](transform func(C, I) (I, error)) Pipe[C, O, I]
```

It simplifies input transformation by:

* Applying a transformation function to the input
* Passing the transformed result to the next pipe
* Stopping the pipeline if the transformation returns an error

### Wye

The `Wye` function splits execution into two parallel branches:

```go
func Wye[C context.Context, O, I any](left, right Pipe[C, O, I]) Pipe[C, O, I]
```

Both branches receive the same input and execute concurrently. If either branch returns an error, execution stops.

### Diverter

The `Diverter` function conditionally selects one or more pipes to execute based on a selector function:

```go
func Diverter[C context.Context, O, I any](selector func(C, I) ([]int, error), pipes ...Pipe[C, O, I]) Pipe[C, O, I]
```

The selector receives the context and input, returning the indices of pipes to execute in parallel. If no pipes are selected, the chain continues immediately.

### Joiner

The `Joiner` function executes all provided pipes in parallel:

```go
func Joiner[C context.Context, O, I any](pipes ...Pipe[C, O, I]) Pipe[C, O, I]
```

All pipes receive the same input and execute concurrently. The chain continues only after all pipes complete successfully.

### Pipeline Composition with Pipe Method

Pipelines can be used as pipes in other pipelines via the `Pipe` method:

```go
func (p *Pipeline[C, O, I]) Pipe(ctx C, sink O, input I, next NextPipe[C, O, I]) error
```

This allows you to compose complex pipeline hierarchies:

```go
// Create a sub-pipeline for validation
validator := pipeline.New(ctx,
    validateInputPipe,
    sanitizeInputPipe,
)

// Create a sub-pipeline for processing
processor := pipeline.New(ctx,
    transformPipe,
    enrichPipe,
)

// Use pipelines as pipes in a main pipeline
mainPipeline := pipeline.New(ctx,
    validator.Pipe,  // Pipeline as a pipe
    processor.Pipe,  // Another pipeline as a pipe
    finalizePipe,
)
```

The `Pipe` method:

* Executes the entire pipeline with the provided sink and input
* Returns any errors from pipeline execution
* Calls the `next` function after successful pipeline completion
* Respects context cancellation

### Pipeline Execution

Pipes execute in order, with each pipe deciding whether to:

1. **Continue the chain** by calling `next(ctx, sink, input)`
2. **Short-circuit** by returning without calling `next()`
3. **Transform data** before passing to `next()`
4. **Handle errors** from downstream pipes

## Examples

### Basic Chain

```go
p := pipeline.New(ctx,
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        sink.Push("before")
        err := next(ctx, sink, input)
        sink.Push("after")
        return err
    },
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        sink.Push("middle")
        return nil
    },
)

// Output: ["before", "middle", "after"]
```

### Short-Circuit

```go
authPipe := func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
    if !isAuthenticated(input) {
        return errors.New("unauthorized")
    }
    return next(ctx, sink, input)
}

processPipe := func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
    // Only executes if authPipe calls next()
    sink.Push("processing " + input)
    return nil
}

p := pipeline.New(ctx, authPipe, processPipe)
```

### Input Transformation

```go
p := pipeline.New(ctx,
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        // Transform and pass modified input
        return next(ctx, sink, strings.ToUpper(input))
    },
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        sink.Push(input) // Receives transformed input
        return nil
    },
)
```

### Using PipeAdapter for Transformations

The `PipeAdapter` helper provides a cleaner way to transform input:

```go
// Create transformation pipes
uppercasePipe := pipeline.PipeAdapter(func(ctx context.Context, input string) (string, error) {
    return strings.ToUpper(input), nil
})

trimPipe := pipeline.PipeAdapter(func(ctx context.Context, input string) (string, error) {
    return strings.TrimSpace(input), nil
})

// Chain transformations
p := pipeline.New(ctx,
    trimPipe,
    uppercasePipe,
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        sink.Push(input) // Receives trimmed and uppercased input
        return nil
    },
)

writer := &Writer{}
p.Process(ctx, writer, "  hello world  ")
// Output: ["HELLO WORLD"]
```

PipeAdapter works with any type:

```go
// Transform integers
multiplyPipe := pipeline.PipeAdapter(func(ctx context.Context, input int) (int, error) {
    return input * 2, nil
})

addPipe := pipeline.PipeAdapter(func(ctx context.Context, input int) (int, error) {
    return input + 10, nil
})

p := pipeline.New(ctx, multiplyPipe, addPipe, processPipe)
p.Process(ctx, writer, 5) // Result: 20 (5 * 2 + 10)
```

### Exchange with Exchanger: Bridge Pipeline Types

Use `Exchange` with an `Exchanger` when you need to reuse a pipe that was written for a different `[Context, Output, Input]` signature:

```go
type IntSink interface {
    Push(int) error
    Flush() error
    Close() error
}

type StringSink interface {
    Push(string) error
    Flush() error
    Close() error
}

intPipe := func(ctx context.Context, sink IntSink, input int, next pipeline.NextPipe[context.Context, IntSink, int]) error {
    if err := sink.Push(input + 1); err != nil {
        return err
    }
    return next(ctx, sink, input+1)
}

type stringSink struct {
    sink StringSink
}

func (s *stringSink) Push(v int) error   { return s.sink.Push(strconv.Itoa(v)) }
func (s *stringSink) Flush() error       { return s.sink.Flush() }
func (s *stringSink) Close() error       { return s.sink.Close() }

exchanged := pipeline.Exchange(
    intPipe,
    pipeline.Exchanger[
        context.Context, StringSink, string,
        context.Context, IntSink, int,
    ]{
        Sink: func(ctx context.Context, sink StringSink, input string) (context.Context, IntSink, int, error) {
            value, err := strconv.Atoi(input)
            if err != nil {
                return nil, nil, 0, err
            }
            return ctx, &stringSink{sink: sink}, value, nil
        },
        Source: func(outerCtx context.Context, outerSink StringSink, _ string, _ context.Context, _ IntSink, innerInput int) (context.Context, StringSink, string, error) {
            return outerCtx, outerSink, strconv.Itoa(innerInput), nil
        },
    },
)

p := pipeline.New(ctx, exchanged, finalPipe)
```

### Using Wye for Parallel Processing

The `Wye` function splits execution into parallel branches:

```go
// Create two independent processing branches
logPipe := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    fmt.Println("Logging:", input)
    return nil
}

metricsPipe := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    // Send metrics
    recordMetric("input_length", len(input))
    return nil
}

// Execute both in parallel
wyePipe := pipeline.Wye(logPipe, metricsPipe)

p := pipeline.New(ctx,
    wyePipe,
    // Continue after both branches complete
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Push("processed: " + input)
        return nil
    },
)
```

### Using Diverter for Conditional Routing

The `Diverter` function routes to different pipes based on input:

```go
// Define handlers for different request types
handleGet := func(ctx context.Context, w *Writer, input Request, next func(context.Context, *Writer, Request) error) error {
    w.Push("GET: " + input.Path)
    return nil
}

handlePost := func(ctx context.Context, w *Writer, input Request, next func(context.Context, *Writer, Request) error) error {
    w.Push("POST: " + input.Path)
    return nil
}

handleDelete := func(ctx context.Context, w *Writer, input Request, next func(context.Context, *Writer, Request) error) error {
    w.Push("DELETE: " + input.Path)
    return nil
}

// Route based on HTTP method
router := pipeline.Diverter(
    func(ctx context.Context, input Request) ([]int, error) {
        switch input.Method {
        case "GET":
            return []int{0}, nil
        case "POST":
            return []int{1}, nil
        case "DELETE":
            return []int{2}, nil
        default:
            return []int{}, nil // No handler selected
        }
    },
    handleGet,
    handlePost,
    handleDelete,
)

p := pipeline.New(ctx, router)
```

You can also select multiple pipes to execute in parallel:

```go
// Select multiple handlers based on flags
diverter := pipeline.Diverter(
    func(ctx context.Context, input Request) ([]int, error) {
        var handlers []int
        if input.RequiresAuth { handlers = append(handlers, 0) }
        if input.RequiresLogging { handlers = append(handlers, 1) }
        if input.RequiresMetrics { handlers = append(handlers, 2) }
        return handlers, nil
    },
    authPipe,
    loggingPipe,
    metricsPipe,
)
```

### Using Joiner for Fan-Out Processing

The `Joiner` function executes multiple pipes in parallel:

```go
// Create independent processing pipes
notifyPipe := func(ctx context.Context, w *Writer, input Order, next func(context.Context, *Writer, Order) error) error {
    sendNotification(input.CustomerID, "Order received")
    return nil
}

inventoryPipe := func(ctx context.Context, w *Writer, input Order, next func(context.Context, *Writer, Order) error) error {
    updateInventory(input.Items)
    return nil
}

analyticsPipe := func(ctx context.Context, w *Writer, input Order, next func(context.Context, *Writer, Order) error) error {
    recordOrderAnalytics(input)
    return nil
}

// Execute all in parallel
joiner := pipeline.Joiner(notifyPipe, inventoryPipe, analyticsPipe)

p := pipeline.New(ctx,
    joiner,
    // Continue after all complete
    func(ctx context.Context, w *Writer, input Order, next func(context.Context, *Writer, Order) error) error {
        w.Push("Order processed: " + input.ID)
        return nil
    },
)
```

### Context Cancellation

```go
ctx, cancel := context.WithCancel(context.Background())

p := pipeline.New(ctx,
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        sink.Push("started")
        cancel() // Cancel context
        return next(ctx, sink, input)
    },
    func(ctx context.Context, sink *Writer, input string, next func(context.Context, *Writer, string) error) error {
        // Will not execute due to cancelled context
        sink.Push("should not execute")
        return nil
    },
)

err := p.Process(ctx, writer, "test")
// err == context.Canceled
```

## Performance

Pipeline uses several optimizations:

* **Pre-built next functions**: Constructed once during pipeline creation
* **Non-recursive**: Avoids stack depth issues with long chains
* **Zero allocations** for empty pipelines

You can run benchmarks for your environment with:

```bash
go test -bench=.
```

## Error Handling

Errors propagate up through the chain:

```go
p := pipeline.New(ctx,
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Write("before")
        err := next(ctx, w, input)
        if err != nil {
            // Handle error from downstream
            w.Write("error handled")
        }
        return err
    },
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        return errors.New("processing failed")
    },
)
```

## Testing

Run tests with coverage:

```bash
go test ./... -v
go test -cover ./...
go test -bench=.
```

The package includes comprehensive tests covering:

* Basic pipeline creation and execution
* Multiple pipe chaining
* Short-circuiting behavior
* Error propagation
* Context cancellation
* Concurrent execution
* Edge cases

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please ensure:

* All tests pass (`go test ./...`)
* Code coverage remains above 90% (`go test -cover ./...`)
* Code follows Go conventions
* New features include tests

## Use Cases

Pipeline is ideal for:

* HTTP middleware chains
* Data processing pipelines
* Event handling
* Stream processing
* Authentication/authorization flows
* Request/response transformation
* Logging and monitoring wrappers
