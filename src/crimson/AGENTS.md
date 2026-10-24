# src/crimson/ — Next-Generation Async OSD (Seastar)

Crimson is the next-generation OSD built on **Seastar**: run-to-completion, shard-per-core, no mutexes, no blocking I/O. Not yet production-ready.

**Never add blocking code**: no `std::mutex`, `std::condition_variable`, `sleep()`, blocking I/O, or standard threading headers.

**Errorator** (`common/errorator.h`) is the core error-handling pattern — statically encodes which errors a function can return. Adding a new error type to a signature cascades through many callers.

**Interruptible futures** (`common/interruptible_future.h`) are used for most PG operations (not plain `seastar::future<>`). Using the wrong type causes hangs or missed interruptions.

**AlienStore** wraps classic BlueStore's blocking calls in a separate thread pool — this is the practical storage backend for Crimson testing. **SeaStore** is experimental.

Cross-core communication uses `seastar::submit_to()` — each core has its own copy of state.
