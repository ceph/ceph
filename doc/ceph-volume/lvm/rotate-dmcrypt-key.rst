.. _ceph-volume-lvm-rotate-dmcrypt-key:

``rotate-dmcrypt-key``
======================

Rotates the dmcrypt (LUKS) passphrase of an encrypted OSD in place, while the
OSD is running. Available for OSDs deployed with ``ceph-volume lvm`` and
``ceph-volume raw`` (``lvm`` accepts ``--osd-id`` or ``--osd-fsid``, ``raw``
requires ``--osd-fsid``). There are two use cases, selected with
``--key-store``, according to where the passphrase is stored:

**Passphrase stored on the monitors (``--key-store mon``, the default:
cephadm and bare-metal deployments).** The passphrase lives in the config-key
store at ``dm-crypt/osd/{osd_fsid}/luks``. A single invocation generates a new
passphrase, installs it and stores it back. Storing needs ``config-key set``
caps, which the OSD's lockbox entity (``client.osd-lockbox.{osd_fsid}``,
get-only) does not have, so pass privileged credentials::

    ceph-volume lvm rotate-dmcrypt-key --osd-id 0 \
        --name client.admin --keyring /etc/ceph/ceph.client.admin.keyring

**Passphrase owned by an external system (``--key-store external``; Rook:
Kubernetes Secret or KMS).** The caller supplies the passphrases via
environment variables and rotates in two phases. Ceph-volume performs only
the LUKS keyslot operations and never touches the config-key store::

    # CEPH_VOLUME_DMCRYPT_SECRET is always the passphrase your key store
    # holds right now; CEPH_VOLUME_NEW_DMCRYPT_SECRET the new passphrase
    export CEPH_VOLUME_DMCRYPT_SECRET=<passphrase in your store>
    export CEPH_VOLUME_NEW_DMCRYPT_SECRET=<new passphrase>
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid> \
        --key-store external --phase stage

    # store the new passphrase in the Secret/KMS and verify it; your store
    # now holds it, so it is the current passphrase from here on
    export CEPH_VOLUME_DMCRYPT_SECRET=<new passphrase>
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid> \
        --key-store external --phase finish

Both models refuse the other one's inputs: ``--key-store mon`` refuses to run
while the passphrase environment variables are set, so a leftover
``CEPH_VOLUME_DMCRYPT_SECRET`` can never silently change which key store is
used.

After phase one both passphrases open every device, so a crash between the
phases can never lock the OSD out.

.. note::
    When the OSD was created, ``prepare`` sent the initial passphrase to
    the monitors, so the config-key store holds a copy of it even when the
    passphrase is owned externally. The external two-phase flow never
    updates that copy: after the rotation it still holds the initial
    passphrase, which no longer opens any keyslot. Anything that reads it
    — an operator, or a ``raw`` activation falling back to the lockbox
    keyring because no passphrase was supplied via the environment — gets
    a dead passphrase. Either have the orchestrator update the copy
    together with its own store (this keeps the fallback working), or
    delete it with ``ceph config-key rm dm-crypt/osd/{osd_fsid}/luks``.

The passphrase is only the key-encryption-key that unlocks a LUKS keyslot.
Ceph historically calls this passphrase the *dmcrypt key*
(``dm-crypt/osd/{osd_fsid}/luks``, ``CEPH_VOLUME_DMCRYPT_SECRET``); in this
document *passphrase* always means that secret, and *key* is reserved for
the LUKS volume key.
Keyslot operations therefore never disturb a running OSD.
All LUKS devices of an OSD (block/db/wal share one passphrase) are rotated as
a set. At every instant of the rotation the passphrase held by the key
store opens every device, so an interrupted rotation is always recoverable by
re-running the command.

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
safely stored. Both key-ownership variants run the same states and differ
only in who performs S3 (storing the new passphrase).

With the passphrase stored on the monitors (the default), one invocation
runs every state. The last column is the passphrase held in the config-key
store at ``dm-crypt/osd/{osd_fsid}/luks``. The stored passphrase is also
enrolled in a LUKS keyslot that makes an interrupted rotation
recoverable::

                                             slot 0   slot 1   config-key (MON)
   start                                     old      -        old
   S0 PRECHECK   stored passphrase opens?    old      -        old
   S1 STAGE      stage current passphrase    old      old      old
   S2 INSTALL    enroll new passphrase       new      old      old
   S3 PERSIST    config-key set + readback   new      old      new
   S4 CLEANUP    wipe slot 1                 new      -        new
   S5 REPORT     keyslot summary             new      -        new

With an externally owned passphrase (e.g. Rook: Kubernetes Secret or KMS),
the same machine is split into two invocations and the *caller* performs
S3 by updating its own key store between them::

                                             slot 0   slot 1   Secret / KMS
   start                                     old      -        old
   phase one: --phase stage
   S0 PRECHECK   env passphrases open?       old      -        old
   S1 STAGE      stage current passphrase    old      old      old
   S2 INSTALL    enroll new passphrase       new      old      old
   caller stores the new passphrase (S3)     new      old      new
   phase two: --phase finish
   S0 PRECHECK   stored passphrase opens?    new      old      new
   S4 CLEANUP    wipe slot 1                 new      -        new
   S5 REPORT     keyslot summary             new      -        new

* **S0 PRECHECK** (read-only): resolve the OSD's LUKS devices — block,
  plus block.db/block.wal when present, all sharing the one stored
  passphrase; refuse TPM2-enrolled and non-LUKS (ceph-disk plain mode)
  OSDs; verify that the passphrase currently held by the key store opens
  every device; refuse devices with active keyslots other than 0 and 1
  unless ``--force``.
* **S1 STAGE**: make the current passphrase valid in slot 1 of every
  device (wipe a stale slot 1, re-add the current passphrase).
* **S2 INSTALL**: on every device, wipe slot 0 and enroll the new
  passphrase there. Both the previous and the new passphrase now open
  every device.
* **S3 PERSIST**: write the new passphrase to the key store and read it
  back to verify.
* **S4 CLEANUP**: verify the stored passphrase opens every device, then
  wipe slot 1 everywhere. Only now does the previous passphrase cease to
  exist.
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

``--phase stage`` runs S0-S2 and exits with both passphrases valid.
``--phase finish`` runs a reduced S0 (the unsupported-target refusals and
the stored-passphrase check, but not the foreign-keyslot gate: slot 1 is
owned by the protocol and is always cleared) and then jumps directly to S4
and S5, skipping S1-S3. S4 assumes that the new passphrase has
already been installed and stored and only the staged previous passphrase
remains to be removed.

Rotating the lockbox cephx secret
---------------------------------

The lockbox entity is a per-OSD cephx identity whose only capability is
reading that OSD's passphrase from the config-key store; it is used by
activation and by nothing else. If its secret is considered compromised, it
should be rotated. Rotating the secret on the Monitors immediately
invalidates the copies kept on the OSD node: the live ``lockbox.keyring`` on
the OSD's tmpfs data directory and, in lvm mode, the persistent
``ceph.cephx_lockbox_secret`` LV tag that activation regenerates the keyring
from. Rotating the lockbox CephX secret is also implemented by ceph-volume,
for both ``lvm`` and ``raw`` OSDs. To do that the new lockbox secret has to
be provided to ceph-volume
via ``CEPH_VOLUME_CEPHX_LOCKBOX_SECRET``. It does not change what
``rotate-dmcrypt-key`` does (it still performs the full passphrase
rotation) but it additionally replaces those node-local copies before the
first config-key access::

    NEW_SECRET=$(ceph auth rotate client.osd-lockbox.<fsid> | awk '/key = /{print $3}')
    CEPH_VOLUME_CEPHX_LOCKBOX_SECRET="$NEW_SECRET" \
        ceph-volume lvm rotate-dmcrypt-key --osd-id 0 --name client.admin \
        --keyring /etc/ceph/ceph.client.admin.keyring

The variable composes with the external two-phase flow the same way — pass
it in phase one::

    NEW_SECRET=$(ceph auth rotate client.osd-lockbox.<fsid> | awk '/key = /{print $3}')
    export CEPH_VOLUME_DMCRYPT_SECRET=<current passphrase>
    export CEPH_VOLUME_NEW_DMCRYPT_SECRET=<new passphrase>
    export CEPH_VOLUME_CEPHX_LOCKBOX_SECRET="$NEW_SECRET"
    ceph-volume raw rotate-dmcrypt-key --osd-fsid <fsid> \
        --key-store external --phase stage

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
``--phase finish`` also serves as the recovery step when a rotation was
interrupted after the new passphrase was already stored — including with
``--key-store mon``, where it saves a full second rotation.

``--force`` is required to rotate a device that has active keyslots other
than 0 and 1 (ceph-volume never creates those); foreign keyslots are left
untouched. This check applies to the main flow only, not to
``--phase finish``.

.. note::
    LUKS keyslots 0 and 1 are owned by the rotation protocol: slot 0 holds
    the canonical passphrase and slot 1 is the staging slot. Do not store
    custom (e.g. break-glass) passphrases in these slots — rotation
    overwrites them, and ``--phase finish`` always clears slot 1 without
    requiring ``--force``. Custom passphrases belong in slots 2 and above,
    which ceph-volume never modifies.

.. note::
    LUKS2 keyslot operations use a memory-hard KDF and can transiently use
    around 1 GiB of RAM per operation. Devices of an OSD are processed
    sequentially; avoid rotating many OSDs of one host in parallel.
