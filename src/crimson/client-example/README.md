# Crimson OSD Client Example

This example demonstrates how to create a direct client to a Crimson OSD using the Crimson messaging system and Seastar framework. Based on `test_async_echo.cc` and `perf_crimson_msgr.cc` from Crimson tools.

## Building

The client is built as part of the main Ceph build system:

```bash
cd /path/to/ceph/build
make -j$(nproc) crimson-osd-client
# or
ninja crimson-osd-client
```

## Usage

```bash
# Basic usage
./bin/crimson-osd-client --osd-addr=v2:127.0.0.1:6800/0

# With verbose logging
./bin/crimson-osd-client --osd-addr=v2:127.0.0.1:6800/0 --verbose
```

## What it does

This client demonstrates:
1. **Crimson Messenger initialization** - Proper setup of Crimson networking
2. **Authentication** - Using DummyAuth for testing
3. **Connection management** - Direct connection to Crimson OSD
4. **Ping operations** - Simple connectivity testing with MPing messages
5. **Message handling** - Asynchronous message processing with Seastar

## Architecture

The client serves as a foundation for building a new RADOS client by demonstrating:
- Proper Ceph configuration initialization
- Crimson messenger setup with correct nonce generation
- Asynchronous message dispatch and handling
- Connection lifecycle management
- Error handling and timeouts

## Requirements

- Running Crimson OSD
- msgr2 protocol support
- Proper Ceph configuration

The client bypasses librados and communicates directly with the OSD using the same messaging protocol that Crimson OSDs use internally.