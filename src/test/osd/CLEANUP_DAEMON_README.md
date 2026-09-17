# MockStore Cleanup Daemon - Crash-Safe Directory Cleanup

## Overview

The MockStore cleanup daemon provides automatic cleanup of test data directories even when tests crash or segfault. This prevents accumulation of `memstore_test_osd*` directories when tests fail unexpectedly.

## How It Works

### Architecture

1. **Fork-Based Design**: When a MockStore is created, it forks a child process (the cleanup daemon)
2. **Parent Monitoring**: The daemon monitors the parent (test) process via a pipe
3. **Automatic Cleanup**: When the parent exits (normally or via crash), the daemon wakes up and deletes the data directory
4. **Normal Exit Optimization**: If the test completes successfully, the parent signals the daemon to skip cleanup (since normal TearDown will handle it)

### Implementation Details

```
Parent Process (Test)          Cleanup Daemon (Child)
     |                                |
     |--- fork() ------------------>  |
     |                                |
     |                                | read(pipe) - blocked
     | Test runs...                   | (waiting for pipe to close)
     |                                |
     | [CRASH/SEGFAULT]               |
     X (process dies)                 |
                                      | pipe closed, read() returns 0
                                      | sleep(10ms)
                                      | verify parent is dead
                                      | remove_all(data_dir)
                                      | exit(0)
```

**Note**: The daemon ALWAYS cleans up the directory, whether the test passes or fails. This ensures no directories are left behind.

### Key Features

- **Crash-Safe**: Works even if the test segfaults, crashes, or is killed
- **No Signal Handlers**: Uses pipe-based communication instead of signals (more reliable)
- **Async-Signal-Safe**: Uses only async-signal-safe functions in the daemon
- **Minimal Overhead**: Daemon is a separate process that doesn't interfere with tests
- **Always Enabled**: Cleanup daemon is always active - no configuration needed
- **Asynchronous Cleanup**: Daemon cleans up in the background without blocking test completion

## Google Test Compatibility

### ✅ Compatible

The cleanup daemon is fully compatible with Google Test:

1. **No Thread Conflicts**: The fork happens in `SetUp()` before any test threads are created
2. **No Signal Handler Conflicts**: The daemon doesn't install signal handlers in the parent process
3. **No Test Interference**: The daemon is a completely separate process that doesn't affect test execution
4. **Works with Death Tests**: Compatible with GTEST's death test framework

### ⚠️ Considerations

1. **Fork Timing**: The daemon must be started before any threads are created. Currently it's started in `MockStore::create()` which is called from `SetUp()`, so this is safe.

2. **Process Cleanup**: The daemon process is automatically reaped when it exits, so no zombie processes are created.

3. **File Descriptor Inheritance**: The daemon closes unnecessary file descriptors after fork to avoid interfering with the parent.

## Usage

### Automatic (Always Enabled)

All MockStore instances created via `MockStore::create()` automatically have the cleanup daemon enabled:

```cpp
auto store = MockStore::create(cct, osd_id);  // Cleanup daemon always enabled
```

The daemon is started automatically and will clean up the directory when the test process exits (normally or via crash). No manual intervention is needed.

## Testing the Cleanup Daemon

### Verify Normal Operation

Run the tests normally - all directories should be cleaned up:

```bash
cd /work/ceph/build
ninja osd_unittests
ls /work/ceph | grep memstore_test  # Should return nothing
```

### Simulate a Crash

To verify crash-safety, you can use the test program:

```bash
cd /work/ceph/build
# Build the test program (if added to CMakeLists.txt)
./bin/test_cleanup_daemon

# The program will:
# 1. Create a MockStore with cleanup daemon
# 2. Print the directory name
# 3. Simulate a crash (SIGSEGV)
# 4. The cleanup daemon should automatically delete the directory
```

After the crash, verify the directory was cleaned up:

```bash
ls /work/ceph | grep memstore_test  # Should return nothing
```

## Implementation Files

- `src/test/osd/MockStore.h` - Cleanup daemon interface
- `src/test/osd/MockStore.cc` - Cleanup daemon implementation
- `src/test/osd/test_cleanup_daemon.cc` - Test program for crash simulation

## Technical Details

### Why Fork Instead of atexit()?

- `atexit()` handlers are NOT called on crashes/segfaults
- Signal handlers have severe restrictions (async-signal-safe only)
- Fork-based approach is more reliable and flexible

### Why Pipe Instead of Signals?

- Pipes are more reliable than signals for process communication
- No risk of signal handler conflicts with GTEST or other libraries
- Simpler implementation (no signal masking, no race conditions)

### Why waitpid() in Daemon?

- Ensures parent is truly dead before cleanup
- Prevents race conditions where parent might still be writing to disk
- Verifies the parent has actually exited (not just closed the pipe)

### Why Not Wait for Daemon in stop_cleanup_daemon()?

- Waiting would block test completion unnecessarily
- The daemon cleans up asynchronously in the background
- Init will reap the daemon process when it exits
- This allows tests to complete faster

## Troubleshooting

### Directories Still Left Behind

If you see `memstore_test_osd*` directories after tests:

1. Check if cleanup daemon is running:
   ```cpp
   std::cout << "Daemon running: " << store->is_cleanup_daemon_running() << std::endl;
   ```

2. Check for orphaned daemon processes:
   ```bash
   ps aux | grep memstore
   ```

3. Check if directories are being created outside MockStore:
   - Only directories created by MockStore::create() have cleanup daemons
   - Manually created directories won't be cleaned up

4. Verify the daemon isn't being killed prematurely:
   - The daemon should outlive the parent process
   - Check system logs for any OOM killer activity

### Tests Hanging

If tests hang, the cleanup daemon might be waiting indefinitely:

1. Check if the pipe is being closed properly
2. Verify `stop_cleanup_daemon()` is called in destructor
3. Check for file descriptor leaks

## Future Enhancements

Possible improvements:

1. **Timeout**: Add a timeout to the daemon so it doesn't wait forever
2. **Logging**: Add optional logging to track daemon activity
3. **Batch Cleanup**: Have one daemon clean up multiple directories
4. **Configurable Delay**: Make the 10ms sleep configurable

## References

- Google Test Documentation: https://google.github.io/googletest/
- POSIX fork() specification
- Async-signal-safe functions: signal-safety(7)