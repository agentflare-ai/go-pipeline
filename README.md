# Pipeline

A generic, type-safe middleware pipeline implementation for Go 1.24+ with context support and storage pooling.

## Overview

Pipeline provides a flexible middleware pattern for chaining processing functions together. It uses Go generics to provide type-safe operations on any writer and input types, with built-in context cancellation and shared storage between pipeline stages.

## Features

- **Generic Types**: Type-safe pipeline with support for any context (`C`), writer (`W`), and input (`I`) types
- **Context Cancellation**: Full support for context-based cancellation at any pipeline stage
- **Storage Pooling**: Built-in sync.Pool for efficient storage management across executions
- **Non-Recursive**: Pre-built next functions eliminate stack depth concerns
- **Short-Circuiting**: Pipes can skip downstream processing by not calling `next()`
- **Error Propagation**: Errors bubble up through the chain automatically

## Installation

```bash
go get github.com/agentflare-ai/pipeline
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/agentflare-ai/pipeline"
)

type Writer struct {
    data []string
}

func (w *Writer) Write(s string) {
    w.data = append(w.data, s)
}

func main() {
    ctx := context.Background()
    
    // Create pipeline with logging and processing pipes
    p := pipeline.New(ctx,
        // Logging pipe
        func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
            fmt.Println("Processing:", input)
            return next(ctx, w, input)
        },
        // Processing pipe
        func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
            w.Write("Processed: " + input)
            return next(ctx, w, input)
        },
    )
    
    writer := &Writer{}
    if err := p.Execute(writer, "hello world"); err != nil {
        panic(err)
    }
    
    fmt.Println(writer.data) // ["Processed: hello world"]
}
```

## Core Concepts

### Pipe Function

A `Pipe` is a middleware function with the signature:

```go
type Pipe[C context.Context, W, I any] func(ctx C, writer W, input I, next func(C, W, I) error) error
```

Each pipe receives:
- `ctx`: Context for cancellation and storage (generic type `C` must satisfy `context.Context`)
- `writer`: The writer instance to operate on (generic type `W`)
- `input`: The input data to process (generic type `I`)
- `next`: Function to invoke the next pipe in the chain

### Pipeline Execution

Pipes execute in order, with each pipe deciding whether to:
1. **Continue the chain** by calling `next(ctx, writer, input)`
2. **Short-circuit** by returning without calling `next()`
3. **Transform data** before passing to `next()`
4. **Handle errors** from downstream pipes

### Storage

Share data between pipes using the built-in storage:

```go
pipe1 := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    // Store data
    if err := pipeline.Store(ctx, "key", "value"); err != nil {
        return err
    }
    return next(ctx, w, input)
}

pipe2 := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    // Load data
    value, ok := pipeline.Load[string](ctx, "key")
    if ok {
        w.Write(value)
    }
    return next(ctx, w, input)
}
```

Storage is:
- Automatically provided in the context during execution
- Isolated between pipeline executions
- Pooled for efficient memory usage

## Examples

### Basic Chain

```go
p := pipeline.New(ctx,
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Write("before")
        err := next(ctx, w, input)
        w.Write("after")
        return err
    },
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Write("middle")
        return nil
    },
)

// Output: ["before", "middle", "after"]
```

### Short-Circuit

```go
authPipe := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    if !isAuthenticated(input) {
        return errors.New("unauthorized")
    }
    return next(ctx, w, input)
}

processPipe := func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
    // Only executes if authPipe calls next()
    w.Write("processing " + input)
    return nil
}

p := pipeline.New(ctx, authPipe, processPipe)
```

### Input Transformation

```go
p := pipeline.New(ctx,
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        // Transform and pass modified input
        return next(ctx, w, strings.ToUpper(input))
    },
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Write(input) // Receives transformed input
        return nil
    },
)
```

### Context Cancellation

```go
ctx, cancel := context.WithCancel(context.Background())

p := pipeline.New(ctx,
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        w.Write("started")
        cancel() // Cancel context
        return next(ctx, w, input)
    },
    func(ctx context.Context, w *Writer, input string, next func(context.Context, *Writer, string) error) error {
        // Will not execute due to cancelled context
        w.Write("should not execute")
        return nil
    },
)

err := p.Execute(writer, "test")
// err == context.Canceled
```

## Performance

Pipeline uses several optimizations:

- **Pre-built next functions**: Constructed once during pipeline creation
- **Storage pooling**: `sync.Pool` for storage maps reduces allocations
- **Non-recursive**: Avoids stack depth issues with long chains
- **Zero allocations** for empty pipelines

Benchmarks on a typical 3-pipe chain:
```
BenchmarkPipeline_Execute-8    5000000    250 ns/op    0 allocs/op
BenchmarkPipeline_Storage-8    3000000    420 ns/op    0 allocs/op
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
- Basic pipeline creation and execution
- Multiple pipe chaining
- Short-circuiting behavior
- Error propagation
- Storage isolation
- Context cancellation
- Concurrent execution
- Edge cases

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please ensure:
- All tests pass (`go test ./...`)
- Code coverage remains above 90% (`go test -cover ./...`)
- Code follows Go conventions
- New features include tests

## Use Cases

Pipeline is ideal for:
- HTTP middleware chains
- Data processing pipelines
- Event handling
- Stream processing
- Authentication/authorization flows
- Request/response transformation
- Logging and monitoring wrappers
