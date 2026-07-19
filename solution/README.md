An in-process task queue where a caller enqueues jobs (a handler + payload); a fixed pool of worker threads runs them with a configurable concurrency limit, delayed execution, retry with exponential backoff, a dead-letter queue, and graceful shutdown
there are no third party binary crates to be installed hence it's enough to install rust toolchain and run the code.

## Setup

Requires a Rust toolchain (1.63+, developed on 1.95). Install via

install rust via curl

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Run — one command

From inside the `solution/` folder:

```sh
cargo run --release
```

This runs the demo, which exercises all seven requirements with millisecond-timestamped console output so the timing-dependent behavior is visible, and asserts each one as it goes