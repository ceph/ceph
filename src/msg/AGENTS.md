# src/msg/ — Messaging Layer

## Purpose

The `msg/` directory implements Ceph's network communication framework. All inter-daemon and client-daemon communication flows through the Messenger abstraction. The primary implementation is `AsyncMessenger`, which provides asynchronous, event-driven networking with support for two wire protocols: v1 (legacy) and v2 (msgr2, with encryption and compression).

Daemons implement the `Dispatcher` interface to receive messages. The messaging layer handles connection lifecycle, authentication, message serialization, and flow control.

## Key Files

| File | Role |
|------|------|
| `Messenger.h/cc` | Abstract base class. Daemons create a Messenger, bind to an address, add dispatchers, and start it. |
| `Message.h/cc` | Base class for all messages. Provides encode/decode, priority, routing. |
| `MessageRef.h` | `MessageRef` — reference-counted message pointer (`boost::intrusive_ptr<Message>`) |
| `Dispatcher.h` | Interface that daemons implement to handle incoming messages (`ms_dispatch()`, `ms_handle_connect()`, etc.) |
| `Connection.h/cc` | Represents a connection to a peer. Used to send messages and track connection state. |
| `DispatchQueue.h/cc` | Queues messages for delivery to dispatchers |
| `Policy.h` | Connection policies (lossy, lossless, stateful, stateless) |
| `SimplePolicyMessenger.h` | Convenience base for messengers with simple per-type policies |
| `msg_types.h/cc` | `entity_addr_t`, `entity_name_t`, `entity_inst_t` — network address and identity types |
| `compressor_registry.h/cc` | On-wire message compression configuration |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `async/` | Asynchronous messenger implementation (the production messenger) |

### async/ Key Files
| File | Role |
|------|------|
| `AsyncMessenger.h/cc` | Production async messenger — event-loop based, one worker thread per connection set |
| `AsyncConnection.h/cc` | Per-connection state machine and I/O |
| `Protocol.h/cc` | Base class for wire protocols |
| `ProtocolV1.h/cc` | Legacy wire protocol (being phased out) |
| `ProtocolV2.h/cc` | Modern msgr2 protocol with encryption, compression, and multiplexing |
| `frames_v2.h/cc` | Protocol v2 frame format definitions |
| `Event.h/cc` | Event loop abstraction |
| `EventEpoll.h/cc` | Linux epoll backend |
| `EventKqueue.h/cc` | macOS/BSD kqueue backend |
| `PosixStack.h/cc` | POSIX socket networking stack |
| `Stack.h/cc` | Network stack abstraction |
| `crypto_onwire.h/cc` | On-wire encryption (AES-GCM) |
| `compression_onwire.h/cc` | On-wire compression |
| `rdma/` | RDMA transport (optional) |
| `dpdk/` | DPDK transport (optional) |

## Patterns and Idioms

### Core Patterns
- **Sending**: `messenger->connect_to(type, addr)->send_message(new MFoo(...))`
- **Receiving**: daemons implement `Dispatcher::ms_dispatch2()` and switch on `m->get_type()` (MSG_* constants)
- **Downcasting**: use `ref_cast<MType>(m)` — messages are reference-counted via `MessageRef`, never `delete` them
- **Connection policies**: `lossy` (may drop, e.g., heartbeats), `lossless` (reliable, e.g., client-to-OSD), `stateless`

## Dependencies

Uses `include/`, `common/`, `auth/` (connection handshake), `compressor/` (on-wire compression).

## Navigation Hints

- To trace how a specific message is handled, find its `MSG_*` constant (in `include/ceph_fs.h` or `include/msgr.h`), then grep for it in the target daemon's dispatch method
- To understand the wire format, read `async/frames_v2.h` for msgr2
- To add a new message type, create a header in `src/messages/`, assign a `MSG_*` constant, and add handling in the relevant daemon's dispatcher

## Gotchas

- Protocol v1 is legacy and being phased out. New features should target v2 only.
- `AsyncMessenger` uses worker threads with event loops. Blocking in a dispatch handler blocks that worker — use Finishers or thread pools for heavy work.
- Connection failures trigger `ms_handle_reset()` / `ms_handle_remote_reset()` callbacks. Daemons must handle these to clean up state.
- Messages carry a `ref_t` (intrusive pointer). Once dispatched, the message is owned by the handler — don't store raw pointers to messages without taking a reference.
