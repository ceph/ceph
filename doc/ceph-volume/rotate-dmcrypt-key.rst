.. _ceph-volume-rotate-dmcrypt-key:

``rotate-dmcrypt-key``
======================

Rotates the dmcrypt (LUKS) passphrase of an encrypted OSD in place, while the
OSD is running. All LUKS devices of an OSD (block/db/wal share one passphrase)
are rotated as a set.

Which subcommand to use follows from how the OSD was deployed, because the two
deployment methods keep the passphrase in different places.

.. note::
    Rotating is only meaningful once the monitors run the fix for
    CVE-2026-50152. Until then the new passphrase is exposed in the same way
    the previous one was, so update the monitors first.

The passphrase is only the key-encryption-key that unlocks a LUKS keyslot.
Ceph historically calls this passphrase the *dmcrypt key*
(``dm-crypt/osd/{osd_fsid}/luks``, ``CEPH_VOLUME_DMCRYPT_SECRET``); in this
document *passphrase* always means that secret, and *key* is reserved for
the LUKS volume key. Keyslot operations therefore never disturb a running
OSD.

``lvm``: the passphrase lives on the monitors
---------------------------------------------

``ceph-volume lvm prepare --dmcrypt`` generates the passphrase itself and
sends it to the monitors, where it is kept in the config-key store at
``dm-crypt/osd/{osd_fsid}/luks``. Rotating it is a single invocation:
ceph-volume generates a new passphrase, installs it on every device and stores
it back.

Storing needs ``config-key set`` caps, which the OSD's lockbox entity
(``client.osd-lockbox.{osd_fsid}``) does not have, because it may only read
that one entry. Pass privileged credentials::

    ceph-volume lvm rotate-dmcrypt-key --osd-id 0 \
        --name client.admin --keyring /etc/ceph/ceph.client.admin.keyring

The OSD is named with either ``--osd-id`` or ``--osd-fsid``.

``raw``: the passphrase is owned by the caller
----------------------------------------------

``ceph-volume raw prepare --dmcrypt`` takes the passphrase from the
environment, so whoever deployed the OSD owns it (Rook, for instance, in a
Kubernetes Secret or a KMS). Rotating takes it the same way. ceph-volume
performs only the LUKS keyslot operations here and never writes to the
config-key store.

The rotation runs as two invocations, so that the caller can store the new
passphrase at a moment when both passphrases still open every device. Which of
the two an invocation is follows from the environment::

    # CEPH_VOLUME_DMCRYPT_SECRET is always the passphrase your key store
    # holds right now. Setting CEPH_VOLUME_NEW_DMCRYPT_SECRET as well
    # installs that passphrase and keeps the current one valid.
    export CEPH_VOLUME_DMCRYPT_SECRET=<passphrase in your store>
    export CEPH_VOLUME_NEW_DMCRYPT_SECRET=<new passphrase>
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid>

    # store the new passphrase in the Secret/KMS and verify it; your store
    # now holds it, so it is the current passphrase from here on

    # leaving CEPH_VOLUME_NEW_DMCRYPT_SECRET unset removes the previous
    # passphrase
    export CEPH_VOLUME_DMCRYPT_SECRET=<new passphrase>
    unset CEPH_VOLUME_NEW_DMCRYPT_SECRET
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid>

Between the two invocations both passphrases open every device, so a crash
there can never lock the OSD out. The second invocation refuses a passphrase
that opens the staging slot rather than the canonical one, which is the state
an interrupted first invocation leaves behind: the rotation is not finished,
and removing the staging slot there would leave the canonical slot holding a
passphrase nobody has.

``lvm rotate-dmcrypt-key`` refuses to run while either passphrase environment
variable is set, so a leftover ``CEPH_VOLUME_DMCRYPT_SECRET`` cannot reach a
flow that does not expect one.

.. note::
    When the OSD was created, ``raw prepare`` sent the initial passphrase to
    the monitors as well, so the config-key store holds a copy of it even
    though the passphrase is owned externally. ``raw rotate-dmcrypt-key``
    never updates that copy: after the rotation it still holds the initial
    passphrase, which no longer opens any keyslot. Anything that reads it, an
    operator or a ``raw`` activation falling back to the lockbox keyring
    because no passphrase was supplied via the environment, gets a dead
    passphrase. Either have the orchestrator update the copy together with its
    own store (this keeps the fallback working), or delete it with ``ceph
    config-key rm dm-crypt/osd/{osd_fsid}/luks``.

.. note::
    Rotating the passphrase does not rotate the LUKS *volume key*. If an
    attacker may have had access to the raw device in addition to the
    passphrase, use ``cryptsetup reencrypt`` (or redeploy the OSD) instead.

.. note::
    TPM2-enrolled OSDs are refused: their passphrase is never stored on the
    monitors. ceph-disk (``simple``) OSDs are not supported.

The rotation state machine
--------------------------

A rotation is a fixed sequence of states, applied to all LUKS devices of
the OSD as a set. In the diagrams below, ``slot 0`` and ``slot 1`` are two
LUKS keyslots of every device. A keyslot stores a copy of the device's
volume key, locked with exactly one passphrase. A cell in the diagram
shows which passphrase currently opens that slot: the ``old`` one, the
``new`` one, or none (``-``, slot empty). Outside a rotation only slot 0
is active, opened by the passphrase held in the key store (the monitor
config-key store, or the external Secret/KMS). Slot 1 is used only during
a rotation, to keep the previous passphrase valid until the new one is
safely stored. Both subcommands run the same states and differ only in who
performs S3 (storing the new passphrase).

With ``lvm``, one invocation runs every state. The last column is the
passphrase held in the config-key store at
``dm-crypt/osd/{osd_fsid}/luks``. The stored passphrase is also enrolled in
a LUKS keyslot that makes an interrupted rotation recoverable::

                                             slot 0   slot 1   config-key (MON)
   start                                     old      -        old
   S0 PRECHECK   stored passphrase opens?    old      -        old
   S1 STAGE      stage current passphrase    old      old      old
   S2 INSTALL    enroll new passphrase       new      old      old
   S3 PERSIST    config-key set + readback   new      old      new
   S4 CLEANUP    wipe slot 1                 new      -        new
   S5 REPORT     keyslot summary             new      -        new

With ``raw``, the same machine is split into two invocations and the
*caller* performs S3 by updating its own key store between them::

                                             slot 0   slot 1   Secret / KMS
   start                                     old      -        old
   first invocation (CEPH_VOLUME_NEW_DMCRYPT_SECRET set)
   S0 PRECHECK   env passphrases open?       old      -        old
   S1 STAGE      stage current passphrase    old      old      old
   S2 INSTALL    enroll new passphrase       new      old      old
   caller stores the new passphrase (S3)     new      old      new
   second invocation (CEPH_VOLUME_NEW_DMCRYPT_SECRET unset)
   S0 PRECHECK   stored passphrase opens?    new      old      new
   S4 CLEANUP    wipe slot 1                 new      -        new
   S5 REPORT     keyslot summary             new      -        new

* **S0 PRECHECK** (read-only): resolve the OSD's LUKS devices, block plus
  block.db/block.wal when present, all sharing the one stored passphrase;
  refuse TPM2-enrolled and non-LUKS (ceph-disk plain mode) OSDs; verify that
  the passphrase currently held by the key store opens every device; refuse
  devices with active keyslots other than 0 and 1 unless ``--force``.
* **S1 STAGE**: make the current passphrase valid in slot 1 of every
  device (wipe a stale slot 1, re-add the current passphrase).
* **S2 INSTALL**: on every device, wipe slot 0 and enroll the new
  passphrase there. Both the previous and the new passphrase now open
  every device.
* **S3 PERSIST**: write the new passphrase to the key store and read it
  back to verify.
* **S4 CLEANUP**: verify the stored passphrase opens every device, then
  wipe slot 1 everywhere. Only now does the previous passphrase cease to
  exist. The second ``raw`` invocation enters here, and additionally
  requires the passphrase to be the one in slot 0.
* **S5 REPORT**: print the active keyslots of every device.

Crash windows
~~~~~~~~~~~~~

Each state stores its result in the LUKS header itself, so an interruption
always lands on one of the rows below. In every one of them the passphrase
held by the key store opens every LUKS device, which is what makes a re-run
safe. The old passphrase is wiped only in S4, once the header and the key
store hold the same new passphrase::

   interrupted after   slot 0   slot 1   key store   the stored passphrase
                                                     opens the device via
   S1 STAGE            old      old      old         slot 0
   S2 INSTALL          new      old      old         slot 1
   S3 PERSIST          new      old      new         slot 0
   S4 CLEANUP          new      -        new         slot 0

A re-run probes the header with ``cryptsetup --test-passphrase`` and
converges from any of these states; no state is kept outside the header.

The first ``raw`` invocation runs S0-S2 and exits with both passphrases valid.
The second runs a reduced S0 (the unsupported-target refusals and the
stored-passphrase check, but not the foreign-keyslot gate: slot 1 is owned by
the protocol and is always cleared) and then jumps directly to S4 and S5,
skipping S1-S3. S4 assumes that the new passphrase has already been installed
and stored and that only the staged previous passphrase remains to be removed.

Rotating the lockbox cephx secret
---------------------------------

The lockbox entity is a per-OSD cephx identity whose only capability is
reading that OSD's passphrase from the config-key store; it is used by
activation and by nothing else. If its secret is considered compromised, it
should be rotated. Rotating the secret on the Monitors immediately
invalidates the copies kept on the OSD node: the live ``lockbox.keyring`` on
the OSD's tmpfs data directory and, in lvm mode, the persistent
``ceph.cephx_lockbox_secret`` LV tag that activation regenerates the keyring
from. ceph-volume does not rotate the cephx secret itself, ``ceph auth
rotate`` does; what ``rotate-dmcrypt-key`` can do, for both ``lvm`` and
``raw`` OSDs, is propagate an already-rotated secret to those node-local
copies. Hand it the new secret via ``CEPH_VOLUME_CEPHX_LOCKBOX_SECRET``. It
does not change what ``rotate-dmcrypt-key`` does otherwise (it still performs
the full passphrase rotation) but it additionally replaces those copies
before the first config-key access::

    NEW_SECRET=$(ceph auth rotate client.osd-lockbox.<fsid> | awk '/key = /{print $3}')
    CEPH_VOLUME_CEPHX_LOCKBOX_SECRET="$NEW_SECRET" \
        ceph-volume lvm rotate-dmcrypt-key --osd-id 0 --name client.admin \
        --keyring /etc/ceph/ceph.client.admin.keyring

The variable composes with the ``raw`` flow the same way. Pass it in the
first invocation::

    NEW_SECRET=$(ceph auth rotate client.osd-lockbox.<fsid> | awk '/key = /{print $3}')
    export CEPH_VOLUME_DMCRYPT_SECRET=<current passphrase>
    export CEPH_VOLUME_NEW_DMCRYPT_SECRET=<new passphrase>
    export CEPH_VOLUME_CEPHX_LOCKBOX_SECRET="$NEW_SECRET"
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid>

.. note::
    ``raw`` OSDs keep no LV tags, so their only node-local copy is the
    keyring on the tmpfs data directory, refreshed in place and lost on the
    next unmount. That keyring matters only when
    ``CEPH_VOLUME_DMCRYPT_SECRET`` is unset, because activation then reads
    the passphrase from the monitors and authenticates as the lockbox entity
    with the keyring it expects at
    ``/var/lib/ceph/osd/{cluster}-{osd_id}/lockbox.keyring``. ``raw``
    activation never recreates that ``lockbox.keyring``, so whoever drives
    activation has to place one holding the *current* lockbox secret::

        ceph auth get client.osd-lockbox.<fsid> \
            -o /var/lib/ceph/osd/<cluster>-<id>/lockbox.keyring

Recovery and edge flags
-----------------------

The command is safely re-runnable after an interruption at any point; it
probes the keyslot state with ``cryptsetup --test-passphrase`` and converges.
For ``lvm`` this holds even when the interruption came after the new
passphrase had already been stored: the re-run reads it back from the
monitors, stages it and rotates to a further passphrase, which costs one extra
passphrase but needs no special invocation. For ``raw``, re-running the second
invocation is idempotent.

Concurrent invocations against the same OSD are refused through a lock file in
``/run/ceph-volume``. Callers that run ceph-volume in a container have to share
that directory with the host for the lock to have any effect, or serialize the
invocations themselves.

Monitor calls are killed after 120 seconds, so an unreachable or wedged
quorum cannot hang the rotation process; interrupting a rotation at any
point, including while a monitor call is pending, is safe, per the state
machine above. S3 does not trust the write: only if the store returns what
was just written may S4 wipe the previous passphrase. A ``config-key set``
that fails or never lands therefore aborts the rotation with the previous
passphrase still in the store, still opening every device. The read-back
detects a corrupted write as well, but it has no ability to correct one. The
entry holds a single passphrase, so a system fault that overwrites it with
garbage loses the previous one. A caller that would rather commit and verify
on its own terms can use the ``raw`` flow and run the second invocation only
once it is satisfied.

``--force`` is required to rotate a device that has active keyslots other
than 0 and 1 (ceph-volume never creates those); foreign keyslots are left
untouched. This check applies to the first invocation only, not to the second.

.. note::
    LUKS keyslots 0 and 1 are owned by the rotation protocol: slot 0 holds
    the canonical passphrase and slot 1 is the staging slot. Do not store
    custom (e.g. break-glass) passphrases in these slots, because rotation
    overwrites them and the second ``raw`` invocation always clears slot 1
    without requiring ``--force``. Custom passphrases belong in slots 2 and
    above, which ceph-volume never modifies.

.. note::
    LUKS2 keyslot operations use a memory-hard KDF and can transiently use
    around 1 GiB of RAM per operation. Devices of an OSD are processed
    sequentially; avoid rotating many OSDs of one host in parallel.
