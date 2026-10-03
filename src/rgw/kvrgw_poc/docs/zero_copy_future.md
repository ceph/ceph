# Zero-copy `FdbFuture`

Design and semantics for extending `FdbFuture` so callers can inspect a Get result without copying into `std::string`. **Not implemented.** Current type lives in `backend/src/kv_store.hpp`.

## Problem

`KvTransaction::kv_wait_get` calls `fdb_future_get_value` then copies bytes into `std::string`. FDB already owns that buffer for the lifetime of the `FDBFuture*`. The caller already owns `FdbFuture`, so the copy is unnecessary when the value is only used before the future is destroyed.

`create_bucket` is the motivating case: it only needs “does `B:` exist?” and an 8-byte LE counter. It never needs a heap string.

## FDB lifetime

`fdb_future_get_value` returns a pointer valid until:

- the `FDBFuture` is destroyed, or
- another get method is called on that future.

A `std::string_view` into that pointer is valid only while the wrapping `FdbFuture` object still owns that `FDBFuture*`. Do not return the view from a function that destroys the future, store it in a cache, or use it after move-from of the source.

## Class shape

Keep existing: default / `FDBFuture*` ctor, destructor (`fdb_future_destroy`), move-only, `raw()`, `operator bool()`, `is_ready()`.

Add cached Get-result fields and:

| Method | Role |
|--------|------|
| `fdb_error_t wait()` | One consume step: `block_until_ready` then `fdb_future_get_value`. Idempotent if already waited successfully. |
| `bool present() const` | Key exists (including zero-length value). |
| `size_t size() const` | Byte length; `0` if `!present()`. |
| `std::string_view view() const` | Points at FDB’s buffer; empty if `!present()`. |

`is_ready()` stays a non-blocking poll. It must **not** call `get_value` or `wait()`.

Suggested data members:

```
FDBFuture*     f_
fdb_error_t    err_           // last wait error; 0 on success
fdb_bool_t     present_
const uint8_t* value_         // FDB-owned; nullptr if !present_
int            value_length_
bool           waited_
```

Move ctor/assign must transfer these fields and clear them on the source (`waited_ = false`, `value_ = nullptr`, …) so a moved-from object does not expose a dangling view. Move-assign of a waited destination destroys the old `FDBFuture*` first (that invalidates any prior `view()` on `*this`).

## `wait()` semantics

1. If `waited_` is already true, return `err_` (0 if the previous wait succeeded).
2. If `f_ == nullptr`, return a non-zero error; leave `waited_` false.
3. `fdb_future_block_until_ready(f_)`. On error, store `err_`, leave `waited_` false, return `err_`.
4. `fdb_future_get_value(f_, &present_, &value_, &value_length_)`. On error, store `err_`, leave `waited_` false, return `err_`.
5. Set `waited_ = true`, `err_ = 0`, return 0.

`get_value` is called at most once per future. Callers must check `wait()`’s return before `present()` / `size()` / `view()`.

On failure, `waited_` stays false so those getters `assert` rather than looking like “key missing.”

## `present()` / `size()` / `view()`

Each begins with `assert(waited_)`.

- **Missing key:** `present() == false`, `size() == 0`, `view()` empty. Not an assert.
- **Present empty value:** `present() == true`, `size() == 0`, `view()` empty. FDB allows a zero-byte value. Do **not** use `size() > 0` as “exists.”
- **Present with bytes:** `present() == true`, `size() > 0`, `view()` covers `[value_, value_length_)`.

Calling getters before a successful `wait()` is a programming error (assert), not “key absent.”

## What not to do

- Implicit `wait()` inside `present()` / `size()` / `view()` (hidden blocking and hidden errors).
- Treating `value_length == 0` as missing.
- Copying into `std::string` inside `wait()` (defeats the point). Copy at the call site only if data must outlive `FdbFuture` (e.g. stack `uint64_t` via `memcpy` from `view()`).

## Relation to `kv_wait_get`

Today `kv_wait_get(FdbFuture&)` blocks, copies to `std::string`, returns `expected<optional<string>, fdb_error_t>`. After this design, callers that own the future should `wait()` + `present()` / `view()` instead.

`kv_wait_get` can remain as a convenience that copies (range-scan rows, values stored past the future). Hot paths should not use it when a stack copy or a presence check is enough.

`kv_get` (async get + wait + copy) unchanged until those call sites are converted.

## `create_bucket` (intended use)

```
auto f_existing = tr->kv_async_get(bucket_key.view());
auto f_counter  = tr->kv_async_get(counter_key.view());

if (fdb_error_t e = f_existing.wait()) { /* retry / return fdb_to_error */ }
if (f_existing.present()) return KVRGW_ERR_OK;   // idempotent exists

if (fdb_error_t e = f_counter.wait()) { /* retry / return */ }
uint64_t bucket_num = 1;
if (f_counter.present()) {
  memcpy into stack uint64 LE from f_counter.view(), then + 1
}
```

No `std::string` for either Get. Futures destroyed at end of the attempt; views must not be used after that.

## Retry

A retriable error from `wait()` still poisons the FDB transaction. Do not `wait()` again on the same `KvTransaction` without a new `begin_transaction()` (this codebase does not use `fdb_transaction_on_error`). New attempt → new futures.

## Out of scope

- Range-scan futures (`get_keyvalue_array`) — different FDB get API; not this wrapper.
- Changing `kv_async_get` (still returns `FdbFuture` wrapping `fdb_transaction_get`).
- Implementation / call-site conversion.
