# src/include/ — Shared Header Files

The fundamental layer that everything depends on.

## Serialization

All on-wire and on-disk structures use versioned encoding from `encoding.h`:
```cpp
void encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  encode(field1, bl);
  encode(field2, bl);   // added in v2
  ENCODE_FINISH(bl);
}
void decode(bufferlist::const_iterator& p) {
  DECODE_START(2, p);
  decode(field1, p);
  if (struct_v >= 2) { decode(field2, p); }  // guard new fields
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(MyType)
```
`denc.h` is a newer zero-copy variant for performance-critical types — don't mix both for the same type.

## bufferlist

`bufferlist` is NOT a contiguous buffer. `bl.c_str()` may copy. Use iterators for decoding: `auto it = bl.cbegin(); decode(obj, it)`.

## Gotchas

- Feature bits in `ceph_features.h` are a limited resource. New features should prefer capability negotiation over adding feature bits.
- `MSG_*` constants must be unique. When adding a new message type, choose the next available number.
