# src/msg/ — Messaging Layer

Production messenger is `AsyncMessenger` in `async/`. Two wire protocols: v1 (legacy, being phased out — new features target v2 only) and v2 (`ProtocolV2.h/cc`, with encryption and compression).

Messages are reference-counted via `MessageRef`. Use `ref_cast<MType>(m)` to downcast — never `delete` a message.

**Do not block in a dispatch handler** — it blocks the worker thread's event loop. Use Finishers or thread pools for heavy work.
