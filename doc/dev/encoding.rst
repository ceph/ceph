
Serialization (encode/decode)
=============================

When a structure is sent over the network or written to disk, it is
encoded into a string of bytes. Usually (but not always -- multiple
serialization facilities coexist in Ceph) serializable structures
have ``encode`` and ``decode`` methods that write and read from
``bufferlist`` objects representing byte strings.

Terminology
-----------
It is best to think not in the domain of daemons and clients but
encoders and decoders. An encoder serializes a structure into a bufferlist
while a decoder does the opposite.

Encoders and decoders can be referred collectively as dencoders.

Dencoders (both encoders and docoders) live within daemons and clients.
For instance, when an RBD client issues an IO operation, it prepares
an instance of the ``MOSDOp`` structure and encodes it into a bufferlist
that is put on the wire.
An OSD reads these bytes and decodes them back into an ``MOSDOp`` instance.
Here encoder was used by the client while decoder by the OSD. However,
these roles can swing -- just imagine handling of the response: OSD encodes
the ``MOSDOpReply`` while RBD clients decode.

Encoder and decoder operate accordingly to a format which is defined
by a programmer by implementing the ``encode`` and ``decode`` methods.

Principles for format change
----------------------------
It is not unusual that the format of serialization changes. This
process requires careful attention from during both development
and review.

The general rule is that a decoder must understand what had been
encoded by an encoder. Most of the problems come from ensuring
that compatibility continues between old decoders and new encoders
as well as new decoders and old decoders. One should assume
that -- if not otherwise derogated -- any mix (old/new) is
possible in a cluster. There are 2 main reasons for that:

1. Upgrades. Although there are recommendations related to the order
   of entity types (mons/osds/clients), it is not mandatory and
   no assumption should be made about it.
2. Huge variability of client versions. It was always the case
   that kernel (and thus kernel clients) upgrades are decoupled
   from Ceph upgrades. Moreover, proliferation of containerization
   bring the variability even to e.g. ``librbd`` -- now user space
   libraries live on the container own.

With this being said, there are few rules limiting the degree
of interoperability between dencoders:

* ``n-2`` for dencoding between daemons,
* ``n-3`` hard requirement for client-involved scenarios,
* ``n-3..``  soft requirements for clinet-involved scenarios. Ideally
  every client should be able to talk any version of daemons.

As the underlying reasons are the same, the rules dencoders
follow are virtually the same as for deprecations of our features
bits. See the ``Notes on deprecation`` in ``src/include/ceph_features.h``.

Frameworks
----------
Currently multiple genres of dencoding helpers co-exist.

* encoding.h (the most proliferated one),
* denc.h (performance optimized, seen mostly in ``BlueStore``),
* the `Message` hierarchy.

Although details vary, the interoperability rules stay the same.

Adding a field to a structure
-----------------------------

You can see examples of this all over the Ceph code, but here's an
example:

.. code-block:: cpp

    class AcmeClass
    {
        int member1;
        std::string member2;

        void encode(bufferlist &bl)
        {
            ENCODE_START(1, 1, bl);
            ::encode(member1, bl);
            ::encode(member2, bl);
            ENCODE_FINISH(bl);
        }

        void decode(bufferlist::iterator &bl)
        {
            DECODE_START(1, bl);
            ::decode(member1, bl);
            ::decode(member2, bl);
            DECODE_FINISH(bl);
        }
    };

The ``ENCODE_START`` macro writes a header that specifies a *version* and
a *compat_version* (both initially 1).  The message version is incremented
whenever a change is made to the encoding.  The compat_version is incremented
only if the change will break existing decoders -- decoders are tolerant
of trailing bytes, so changes that add fields at the end of the structure
do not require incrementing compat_version.

The ``DECODE_START`` macro takes an argument specifying the most recent
message version that the code can handle.  This is compared with the
compat_version encoded in the message, and if the message is too new then
an exception will be thrown.  Because changes to compat_version are rare,
this isn't usually something to worry about when adding fields.

In practice, changes to encoding usually involve simply adding the desired fields
at the end of the ``encode`` and ``decode`` functions, and incrementing
the versions in ``ENCODE_START`` and ``DECODE_START``.  For example, here's how
to add a third field to ``AcmeClass``:

.. code-block:: cpp

    class AcmeClass
    {
        int member1;
        std::string member2;
        std::vector<std::string> member3;

        void encode(bufferlist &bl)
        {
            ENCODE_START(2, 1, bl);
            ::encode(member1, bl);
            ::encode(member2, bl);
            ::encode(member3, bl);
            ENCODE_FINISH(bl);
        }

        void decode(bufferlist::iterator &bl)
        {
            DECODE_START(2, bl);
            ::decode(member1, bl);
            ::decode(member2, bl);
            if (struct_v >= 2) {
                ::decode(member3, bl);
            }
            DECODE_FINISH(bl);
        }
    };

Note that the compat_version did not change because the encoded message
will still be decodable by versions of the code that only understand
version 1 -- they will just ignore the trailing bytes where we encode ``member3``.

In the ``decode`` function, decoding the new field is conditional: this is
because we might still be passed older-versioned messages that do not
have the field.  The ``struct_v`` variable is a local set by the ``DECODE_START``
macro.

# Into the weeeds

The append-extendability of our dencoders is a result of the forward
compatibility that the ``ENCODE_START`` and ``DECODE_FINISH`` macros bring.

They are implementing extendibility facilities. An encoder, when filling
the bufferlist, prepends three fields: version of the current format,
minimal version of a decoder compatible with it and the total size of
all encoded fields.

.. code-block:: cpp

        /**
         * start encoding block
         *
         * @param v current (code) version of the encoding
         * @param compat oldest code version that can decode it
         * @param bl bufferlist to encode to
         *
         */
        #define ENCODE_START(v, compat, bl)                             \
          __u8 struct_v = v;                                            \
          __u8 struct_compat = compat;                                  \
          ceph_le32 struct_len;                                         \
          auto filler = (bl).append_hole(sizeof(struct_v) +             \
            sizeof(struct_compat) + sizeof(struct_len));                \
          const auto starting_bl_len = (bl).length();                   \
          using ::ceph::encode;                                         \
          do {

The ``struct_len`` field allows the decoder to eat all the bytes that were
left undecoded in the user-provided ``decode`` implementation.
Analogically, decoders tracks how much input has been decoded in the
user-provided ``decode`` methods.

.. code-block:: cpp

        #define DECODE_START(bl)		                        \
          unsigned struct_end = 0;					\
          __u32 struct_len;						\
          decode(struct_len, bl);					\
          ...                                                           \
          struct_end = bl.get_off() + struct_len;			\
          }								\
          do {


Decoder uses this information to discard the extra bytes it does not
understand. Advancing bufferlist is critical as dencoders tend to be nested;
just leaving it intact would work only for the very last ``deocde`` call
in a nested structure.

.. code-block:: cpp

        #define DECODE_FINISH(bl)					\
          } while (false);						\
          if (struct_end) {						\
            ...                                                         \
            if (bl.get_off() < struct_end)				\
              bl += struct_end - bl.get_off();				\
          }


This entire, cooperative mechanism allows encoder (its further revisions)
to generate more byte stream (due to e.g. adding a new field at the end)
and not worry that the residue will crash older decoder revisions.
