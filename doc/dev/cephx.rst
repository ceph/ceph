=====
Cephx
=====

.. _cephx:

Intro
-----

The protocol design looks a lot like kerberos.  The authorizer "KDC"
role is served by the monitor, who has a database of shared secrets
for each entity.  Clients and non-monitor daemons all start by
authenticating with the monitor to obtain tickets, mostly referreed to
in the code as authorizers.  These tickets provide both
*authentication* and *authorization* in that they include a
description of the *capabilities* for the entity, a concise structured
description of what actions are allowed, that can be interpreted and
enforced by the service daemons.

Other references
----------------

- A write-up from 2012 on cephx as it existed at that time by Peter
  Reiher: :ref:`cephx_2012_peter`

Terms
-----

- *monitor(s)*: central authorization authority
- *service*: the set of all daemons of a particular type (e.g., all
  OSDs, all MDSs)
- *client*: an entity or principal that is accessing the service
- *entity name*: the string identifier for a principal
  (e.g. client.admin, osd.123)
- *ticket*: a bit of data that cryptographically asserts identify and
  authorization

- *principal*: a client or daemon, identified by a unique entity_name,
  that shares a secret with the monitor.
- *principal_secret*: principal secret, a shared secret (16 bytes)
  known by the principal and the monitor
- *mon_secret*: monitor secret, a shared secret known by all monitors
- *service_secret*: a rotating secret known by all members of a
  service class (e.g., all OSDs)

- *auth ticket*: a ticket proving identity to the monitors
- *service ticket*: a ticket proving identify and authorization to a
  service

  
Terminology
-----------

``{foo, bar}^secret`` denotes encryption by secret.


Context
-------

The authentication messages described here are specific to the cephx
auth implementation.  The messages are transferred by the Messenger
protocol or by MAuth messages, depending on the version of the
messenger protocol.  See also :ref:`msgr2-protocol`.

An initial (messenger) handshake negotiates an authentication method
to be used (cephx vs none or krb or whatever) and an assertion of what
entity the client or daemon is attempting to authenticate as.

Phase I: obtaining auth ticket
------------------------------

The cephx exchange begins with the monitor knowing who the client
claims to be, and an initial cephx message from the monitor to the
client/principal.::

  a->p : 
    CephxServerChallenge {
      u64 server_challenge     # random (by server)
    }

The client responds by adding its own challenge, and calculating a
value derived from both challenges and its shared key
principal_secret.::

  p->a :
    CephxRequestHeader {
      u16 CEPHX_GET_AUTH_SESSION_KEY
    }
    CephXAuthenticate {
      u8 2                     # 2 means nautilus+
      u64 client_challenge     # random (by client)
      u64 key = {client_challenge ^ server_challenge}^principal_secret   # (roughly)
      blob old_ticket          # old ticket, if we are reconnecting or renewing
      u32 other_keys           # bit mask of service keys we want
    }

Prior to nautilus,::

    CephXAuthenticate {
      u8 1                     # 2 means nautilus+
      u64 client_challenge     # random (by client)
      u64 key = {client_challenge + server_challenge}^principal_secret   # (roughly)
      blob old_ticket          # old ticket, if we are reconnecting or renewing
    }

The monitor looks up principal_secret in database, and verifies the
key is correct.  If old_ticket is present, verify it is valid, and we
can reuse the same global_id.  (Otherwise, a new global_id is assigned
by the monitor.)::

  a->p :
    CephxReplyHeader {
      u16 CEPHX_GET_AUTH_SESSION_KEY
      s32 result (0)
    }
    u8 encoding_version = 1
    u32 num_tickets ( = 1)
    ticket_info           # (N = 1)

plus (for Nautilus and later)::

    u32 connection_secret_len      # in bytes
    connection_secret^session_key
    u32 other_keys_len             # bytes of other keys (encoded)
    other_keys {
      u8 encoding_version = 1
      u32 num_tickets
      service_ticket_info * N      # for each service ticket
    }

where::

    ticket_info {
      u32 service_id       # CEPH_ENTITY_TYPE_AUTH
      u8 msg_version (1)
      {CephXServiceTicket service_ticket}^principal_secret
      {CephxTicketBlob ticket_blob}^existing session_key   # if we are renewing a ticket,
      CephxTicketBlob ticket_blob                          # otherwise
    }

    service_ticket_info {
      u32 service_id       # CEPH_ENTITY_TYPE_{OSD,MDS,MGR}
      u8 msg_version (1)
      {CephXServiceTicket service_ticket}^principal_secret
      CephxTicketBlob ticket_blob
    }

    CephxServiceTicket {
      CryptoKey session_key      # freshly generated (even if old_ticket is present)
      utime_t expiration         # now + auth_mon_ticket_ttl
    }

    CephxTicketBlob {
      u64 secret_id             # which service ticket encrypted this; -1 == monsecret, otherwise service's rotating key id
      {CephXServiceTicketInfo ticket}^mon_secret
    }

    CephxServiceTicketInfo {
      CryptoKey session_key     # same session_key as above
      AuthTicket ticket
    }

    AuthTicket {
      EntityName name           # client's identity, as proven by its possession of principal_secret
      u64 global_id             # newly assigned, or from old_ticket
      utime_t created, renew_after, expires
      AuthCapsInfo       # what client is allowed to do
      u32 flags = 0      # unused
    }

So: for each ticket, principal gets a part that it decrypts with its
secret to get the session_key (CephxServiceTicket).  And the
CephxTicketBlob is opaque (secured by the mon secret) but can be used
later to prove who we are and what we can do (see CephxAuthorizer
below).

For Nautilus+, we also include the service tickets.

The client can infer that the monitor is authentic because it can
decrypt the service_ticket with its secret (i.e., the server has its
secret key).


Phase II: Obtaining service tickets (pre-nautilus)
--------------------------------------------------

Now the client needs the keys used to talk to non-monitors (osd, mds,
mgr).::

  p->a :
    CephxRequestHeader {
      u16 CEPHX_GET_PRINCIPAL_SESSION_KEY
    }
    CephxAuthorizer authorizer      
    CephxServiceTicketRequest {
      u32 keys    # bitmask of CEPH_ENTITY_TYPE_NAME (MGR, OSD, MDS, etc)
    }

where::

    CephxAuthorizer {
      u8 AUTH_MODE_AUTHORIZER (1)
      u64 global_id
      u32 service_id    # CEPH_ENTITY_TYPE_*
      CephxTicketBlob auth_ticket
      {CephxAuthorize msg}^session_key
    }

    CephxAuthorize msg {
      u8 2
      u64 nonce                         # random from client
      bool have_challenge = false       # not used here
      u64 server_challenge_plus_one = 0 # not used here
    }

The monitor validates the authorizer by decrypting the auth_ticket
with ``mon_secret`` and confirming that it says this principal is who
they say they are in the CephxAuthorizer fields.  Note that the nonce
random bytes aren't used here (the field exists for Phase III below).

Assuming all is well, the authorizer can generate service tickets
based on the CEPH_ENTITY_TYPE_* bits in the ``keys`` bitmask.

The response looks like::

    CephxResponseHeader {
      u16 CEPHX_GET_PRINCIPAL_SESSION_KEY
      s32 result (= 0)
    }
    u8 encoding_version = 1
    u32 num_tickets
    ticket_info * N
  
Where, as above,::

    ticket_info {
      u32 service_id      # CEPH_ENTITY_TYPE_{OSD,MGR,MDS}
      u8 msg_version (1)
      {CephXServiceTicket service_ticket}^principal_secret
      CephxTicketBlob ticket_blob
    }

    CephxServiceTicket {
      CryptoKey session_key
      utime_t expiration
    }

    CephxTicketBlob {
      u64 secret_id       # which version of the (rotating) service ticket encrypted this
      {CephXServiceTicketInfo ticket}^rotating_service_secret
    }

    CephxServiceTicketInfo {
      CryptoKey session_key
      AuthTicket ticket
    }

    AuthTicket {
      EntityName name
      u64 global_id
      utime_t created, renew_after, expires
      AuthCapsInfo       # what you are allowed to do
      u32 flags = 0      # unused
    }

This concludes the authentication exchange with the monitor.  The
client or daemon now has tickets to talk to the mon and all other
daemons of interest.


Phase III: Opening a connection to a service
--------------------------------------------

When a connection is opened, an "authorizer" payload is sent::

  p->s :
    CephxAuthorizer {
      u8 AUTH_MODE_AUTHORIZER (1)
      u64 global_id
      u32 service_id    # CEPH_ENTITY_TYPE_*
      CephxTicketBlob auth_ticket
      {CephxAuthorize msg}^session_key
    }

    CephxAuthorize msg {
      u8 2
      u64 nonce               # random from client
      bool have_challenge = false
      u64 server_challenge_plus_one = 0
    }

Note that prior to the Luminous v12.2.6 or Mimic v13.2.2 releases, the
CephxAuthorize msg did not contain a challenge, and consisted only
of::

    CephxAuthorize msg {
      u8 1
      u64 nonce               # random from client
    }
    
The server will inspect the auth_ticket CephxTicketBlob (by decrypting
it with its current rotating service key).  If it is a pre-v12.2.6 or
pre-v13.2.2 client, the server immediately replies with::

  s->p :
    {CephxAuthorizeReply reply}^session_key

where::

    CephxAuthorizeReply {
      u64 nonce_plus_one
    }

Otherwise, the server will respond with a challenge (to prevent replay
attacks)::

  s->p :
    {CephxAuthorizeChallenge challenge}^session_key

where::

    CephxAuthorizeChallenge {
      u64 server_challenge        # random from server
    }

The client decrypts and updates its CephxAuthorize msg accordingly,
resending most of the same information as before::

  p->s :
    CephxAuthorizer {
      u8 AUTH_MODE_AUTHORIZER (1)
      u64 global_id
      u32 service_id    # CEPH_ENTITY_TYPE_*
      CephxTicketBlob auth_ticket
      {CephxAuthorize msg}^session_key
    }

where::

    CephxAuthorize msg {
      u8 2
      u64 nonce                        # (new) random from client
      bool have_challenge = true
      u64 server_challenge_plus_one    # server_challenge + 1
    }

The server validates the ticket as before, and then also verifies the
msg nonce has it's challenge + 1, confirming this is a live
authentication attempt (not a replay).

Finally, the server responds with a reply that proves its authenticity
to the client.  It also includes some entropy to use for encryption of
the session, if it is needed for the mode.::

  s->p :
    {CephxAuthorizeReply reply}^session_key

where::

    CephxAuthorizeReply {
      u64 nonce_plus_one
      u32 connection_secret_length
      connection secret
    }

Prior to nautilus, there is no connection secret::

    CephxAuthorizeReply {
      u64 nonce_plus_one
    }

The client decrypts and confirms that the server incremented nonce
properly and that this is thus a live authentication request and not a
replay.


Rotating service secrets
------------------------

Daemons make use of a rotating secret for their tickets instead of a
fixed secret in order to limit the severity of a compromised daemon.
If a daemon's secret key is compromised by an attacker, that daemon
and its key can be removed from the monitor's database, but the
attacker may also have obtained a copy of the service secret shared by
all daemons.  To mitigate this, service keys rotate periodically so
that after a period of time (auth_service_ticket_ttl) the key the
attacker obtained will no longer be valid.::

  p->a :
    CephxRequestHeader {
      u16 CEPHX_GET_ROTATING_KEY
    }

  a->p :
    CephxReplyHeader {
      u16 CEPHX_GET_ROTATING_KEY
      s32 result = 0
    }
    {CryptoKey service_key}^principal_secret

That is, the new rotating key is simply protected by the daemon's
rotating secret.

Note that, as an implementation detail, the services keep the current
key and the prior key on hand so that the can continue to validate
requests while the key is being rotated.
