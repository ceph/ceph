# metadata sync v2.1

## motivation

"multisite v2" was introduced in Ceph Jewel to replace the old python radosgw-agent. the execution model is based on stackless coroutines, which are a good fit because multisite sync involves a lot of highly-concurrent but short-lived tasks

however, our custom `RGWCoroutine` framework has made it difficult to read, write, test, and debug sync code

each coroutine has to be a separate class that inherits from `RGWCoroutine` to override its pure virtual operate() function. all of the coroutine's logic goes inside of a `reenter()` block, which is a macro based on a switch statement. this hidden switch statement obscures the control flow and complicates the use of local variables and RAII. as a result, most of the local variables have to be member variables instead

these coroutines run in a custom single-threaded scheduler, `RGWCoroutinesManager`, which itself has introduced several bugs

### c++20 coroutines

c++20 added language support for stackless coroutines, which are just normal c++ functions that use new keywords like `co_await` and `co_return`. the compiler handles the messy business of breaking the coroutine function up at its suspension points, moving local variables into the coroutine frame, etc. this eliminates most of problems with `RGWCoroutine` above

### asio

from the very start, asio added support for c++20 coroutines with `asio::co_spawn()` and the `asio::awaitable<T>` return type. like the stackful coroutines (`asio::yield_context`) used by the beast frontend, these stackless coroutines are scheduled and run by the `asio::io_context`

this removes the need for a custom scheduler like `RGWCoroutinesManager` entirely. and unlike `RGWCoroutinesManager`, the `asio::io_context` allows unit tests to step through the suspension points with `io_context::poll()` and `io_context::run_one()`. this makes it easy to set up and test very specific races

## design

the following abstractions are proposed:

* RemoteMetadata: list() and read() metadata objects from a remote zone
* LocalMetadata: write() and remove() metadata objects from the local zone
* RemoteLog: list() entries from a remote mdlog shard, or fetch info() or shard_info() about its mdlogs
* LocalLog: list() or write() entries to a local mdlog shard (copied from the remote mdlog in case of failover)
* FullSyncIndex: write() entries when building the full sync index, or list() them during full sync
* Status: read() or write() global sync status (which period we're on)
* LogStatus: read() or write() sync status for a given mdlog shard, or lock()/unlock() it for exclusive processing

strong abstractions here allow us to write unit tests that mock out any http or rados requests. for example, these mock objects can inject a specific sequence of mdlog entries and verify that the incremental sync function makes the expected calls in response

the abstractions also make it easier to split up work between several developers. all of the sync logic can be written and tested before we have working implementations, and each interface can be implemented separately

once everything is in place, we can start running the existing multisite functional tests

### rados requests

the `neorados` library follows asio's async model, so its functions support c++20 coroutines already

however, metadata sync relies on the `RGWMetadataManager` and `RGWMetadataHandler` classes to read and write metadata to rados using `librados` and `optional_yield`. we'll probably want to add `asio::awaitable` versions of those interfaces for `LocalMetadata`. in the meantime, its c++20 coroutine functions could spawn a stackful coroutine and rely on the existing `optional_yield` support

not all cls clients have been added to `neorados` yet, so `LocalLog` and `LogStatus` may need to add those for `cls_log` and `cls_lock`

### http requests

`RGWHTTPManager` supports stackful coroutines with `optional_yield` and integrates with the `RGWCoroutine` framework, but does not support c++20 coroutines

i've proposed a new async libcurl client in https://github.com/ceph/ceph/pull/58094 for use here. like `neorados`, it follows asio's async model so supports c++20 coroutines and asio's other completion types. and unlike `RGWHTTPManager`, it doesn't require a separate background thread to poll for libcurl completions. it runs on the same `asio::io_context` that's running the metadata sync coroutines

### error handling

if a coroutine spawned by `asio::co_spawn()` exits with an exception, that exception is captured and passed back to `asio::co_spawn()`'s completion handler. these coroutines may also choose to return errors by using a return type like `asio::awaitable<int>`. on completion, that return value is passed back to the completion handler whose function signature is `(std::exception_ptr eptr, int error)`. coroutines may also choose to return `asio::awaitable<void>`, resulting in a completion signature of `(std::exception_ptr eptr)`

because exceptions must be handled in either case, we've found it simplest to rely exclusively on exceptions for error handling in metadata sync. as a result, care must be taken to write exception-safe code

spawned coroutines also support [Per-Op Cancellation](https://www.boost.org/doc/libs/1_82_0/doc/html/boost_asio/overview/core/cancellation.html), which cause the canceled coroutine to exit with an exception

### concurrency patterns

over time, the `RGWCoroutine` framework developed a small library of concurrency primitives so that a parent coroutine could manage several child coroutines running in parallel. examples include the `RGWShardCollectCR` class and macros like `drain_all()` and `yield_spawn_window()`

we've developed similar primitives for use with c++20 coroutines, with a focus on [structured concurrency](https://en.wikipedia.org/wiki/Structured_concurrency)

#### fork-join

sync often needs to spawn several coroutines and wait for all of them to complete. for example, metadata sync spawns a child coroutine for each shard of the mdlog to be polled/processed in parallel

https://github.com/ceph/ceph/pull/50005 added a `spawn_group` class to wait on a group of child coroutines, along with a `parallel_for_each()` algorithm inspired by seastar's

#### bounded concurrency

similarly, incremental sync processes a long stream of mdlog entries by spawning a child coroutine for each. but here, it's important that we limit how many entries we process at a time to:
* limit total memory usage,
* spread load evenly between mdlog shards, and
* limit the amount of work we'd have to redo in case of errors and retries

https://github.com/ceph/ceph/pull/49720 added a `co_throttle` class for this, which limits the total number of child coroutines by blocking before spawning more

#### cls locks

multisite sync uses cls locks to divide up the work between cooperating rgws. sync must only process a given mdlog shard while it holds an exclusive lock on it

to this end, https://github.com/ceph/ceph/pull/49904 added a `with_lease()` algorithm. this spawns a child coroutine once the lock is acquired, continuously renews the lock while it's running, and releases the lock once the child completes. in the meantime, if the lock expires or its renewal fails, the child coroutine is canceled immediately
