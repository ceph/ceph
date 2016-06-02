msgr2 protocol
==============

This is a revision of the legacy Ceph on-wire protocol that was
implemented by the SimpleMessenger.  It addresses performance and
security issues.

Definitions
-----------

* *client* (C): the party initiating a (TCP) connection
* *server* (S): the party accepting a (TCP) connection
* *connection*: an instance of a (TCP) connection between two peers
* *session*: a stateful session between two peers in which message
  exchange is ordered and lossless.  A session might span multiple
  connections if there is an interruption (TCP connection disconnect).
* *frame*: a discrete message sent between the peers.  Each frame
  consists of a tag (type code), payload, and (if signing or
  encryption is enabled) some other fields.  See below for the
  structure.
* *tag*: a single-byte type code associated with a frame.  The tag
  determines the structure of the payload.

Phases
------

A connection has four distinct phases:

#. banner
#. authentication
#. message flow handshake
#. message exchange

Banner
------

Both the client and server, upon connecting, send a banner::

  "ceph %x %x\n", protocol_features_suppored, protocol_features_required

The protocol features are a new, distinct namespace.  Initially no
features are defined or required, so this will be "ceph 0 0\n".

If the remote party advertises required features we don't support, we
can disconnect.

Authentication
--------------

A series of frames between client and server of the form::

  tag byte (TAG_*)
  payload

where the payload is determiend by the tag.

* TAG_AUTH_METHODS (server only): list authentication methods (none, cephx, ...)::

    __le32 num_methods;
    __le32 methods[num_methods];   // CEPH_AUTH_{NONE, CEPHX}

* TAG_AUTH_SET_METHOD (client only): set auth method for this connection::

    __le32 method;

  - The selected auth method determines the sig_size and block_size in any
    subsequent messages (TAG_AUTH_DONE and non-auth messages).

* TAG_AUTH_BAD_METHOD (server only): reject client-selected auth method::

    __le32 method

* TAG_AUTH: client->server or server->client auth message::

    __le32 len;
    method specific payload

* TAG_AUTH_DONE::
    
    __le64 flags
      FLAG_ENCRYPTED  1
      FLAG_SIGNED     2
    signature

  - The client first says AUTH_DONE, and the server replies to
    acknowledge it.


Message frame format
--------------------

Each frame can take one of two forms.  If FLAG_SIGNED or
FLAG_ENCRYPTED has been specified and we have passed the
authentication phase (i.e., we have already sent TAG_AUTH_DONE)::

  confounder (block_size bytes of random garbage)
  __le32 length
  tag byte
  payload
  signature (sig_size bytes)
  more confounder padding (to pad data from start of __le32 length out to block size)

Note that the padding ensures that the total frame (with or without
the leading confounder) is a multiple of the auth method's block_size.
This is usually something like 16 bytes.

If neither FLAG_SIGNED or FLAG_ENCRYPTED is specified, things are simple::

  tag byte
  payload
    
Message flow handshake
----------------------

In this phase the peers identify each other and (if desired) reconnect to
an established session.

* TAG_IDENT: identify ourselves::

    entity_addrvec_t addr(s)
    __u8   my type (CEPH_ENTITY_TYPE_*)
    __le32 protocol version
    __le64 features supported (CEPH_FEATURE_* bitmask)
    __le64 features required (CEPH_FEATURE_* bitmask)
    __le64 flags (CEPH_MSG_CONNECT_* bitmask)
    __le64 cookie (a client identifier, assigned by the sender. unique on the sender.)

  - client will send first, server will reply with same.

* TAG_IDENT_MISSING_FEATURES (server only): complain about a TAG_IDENT with too few features::

    __le64 features we require that peer didn't advertise

* TAG_IDENT_BAD_PROTOCOL (server only): complain about an old protocol version::

    __le32 protocol_version (our protocol version)

* TAG_RECONNECT (client only): reconnect to an established session::

    __le64 cookie
    __le64 global_seq
    __le64 connect_seq
    __le64 msg_seq (the last msg seq received)

* TAG_RECONNECT_OK (server only): acknowledge a reconnect attempt::

    __le64 msg_seq (last msg seq received)

* TAG_RECONNECT_RETRY_SESSION (server only): fail reconnect due to stale connect_seq

* TAG_RECONNECT_RETRY_GLOBAL (server only): fail reconnect due to stale global_seq

* TAG_RECONNECT_WAIT (server only): fail reconnect due to connect race.

  - Indicates that the server is already connecting to the client, and
    that direction should win the race.  The client should wait for that
    connection to complete.

* TAG_START: client is ready to start the message exchange session::

    __le64 flags

  - Sent by client when it is ready.  Server replies with same.
  - No flags are defined now, but we might use them later.
  - I'm not sure this is necessary.

Message exchange
----------------

Once a session is stablished, we can exchange messages.

* TAG_MSG: a message::

    ceph_msg_header2
    front
    middle
    data

  - The ceph_msg_header is modified in ceph_msg_header2 to include an
    ack_seq.  This avoids the need for a TAG_ACK message most of the time.

* TAG_ACK: acknowledge receipt of message(s)::

    __le64 seq

  - This is only used for stateful sessions.

* TAG_KEEPALIVE2: check for connection liveness::

    ceph_timespec stamp

  - Time stamp is local to sender.

* TAG_KEEPALIVE2_ACK: reply to a keepalive2::

    ceph_timestamp stamp

  - Time stamp is from the TAG_KEEPALIVE2 we are responding to.

  
