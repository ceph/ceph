.. _rgw_mfa:

==========================================
RGW Support for Multifactor Authentication
==========================================

The S3 multifactor authentication (MFA) feature allows
users to require the use of one-time password when removing
objects on certain buckets. The buckets need to be configured
with versioning and MFA enabled which can be done through
the S3 API.

Time-based one time password tokens can be assigned to a user
through radosgw-admin. Each token has a secret seed, and a serial
id that is assigned to it. Tokens are added to the user, can
be listed, removed, and can also be re-synchronized.

Multisite
=========

While the MFA IDs are set on the user's metadata, the
actual MFA one time password configuration resides in the local zone's
OSDs. Therefore, in a multi-site environment it is advisable to use
different tokens for different zones.


Terminology
=============

-``TOTP``: Time-based One Time Password

-``token serial``: a string that represents the ID of a TOTP token

-``token seed``: the secret seed that is used to calculate the TOTP

-``totp seconds``: the time resolution that is being used for TOTP generation

-``totp window``: the number of TOTP tokens that are checked before and after the current token when validating token

-``totp pin``: the valid value of a TOTP token at a certain time


Admin commands
==============

Create a new MFA TOTP token
------------------------------------

::

   # radosgw-admin mfa create --uid=<user-id> \
                              --totp-serial=<serial> \
                              --totp-seed=<seed> \
                              [ --totp-seed-type=<hex|base32> ] \
                              [ --totp-seconds=<num-seconds> ] \
                              [ --totp-window=<twindow> ]

List MFA TOTP tokens
---------------------

::

   # radosgw-admin mfa list --uid=<user-id>


Show MFA TOTP token
------------------------------------

::

   # radosgw-admin mfa get --uid=<user-id> --totp-serial=<serial>


Delete MFA TOTP token
------------------------

::

   # radosgw-admin mfa remove --uid=<user-id> --totp-serial=<serial>


Check MFA TOTP token
--------------------------------

Test a TOTP token pin, needed for validating that TOTP functions correctly. ::

   # radosgw-admin mfa check --uid=<user-id> --totp-serial=<serial> \
                             --totp-pin=<pin>


Re-sync MFA TOTP token
--------------------------------

In order to re-sync the TOTP token (in case of time skew). This requires
feeding two consecutive pins: the previous pin, and the current pin. ::

   # radosgw-admin mfa resync --uid=<user-id> --totp-serial=<serial> \
                              --totp-pin=<prev-pin> --totp-pin=<current-pin>


Requests that need a pin
========================

Once MFA delete is enabled on a bucket, RGW requires a valid pin for these S3
requests:

- ``DeleteObject`` with a version ID, which permanently deletes that object
  version. Without a version ID, ``DeleteObject`` adds a delete marker and
  needs no pin.
- ``DeleteObjects`` (multi-object delete) when any of its keys has a version
  ID.
- ``PutBucketVersioning`` when it changes the versioning state between
  ``Enabled`` and ``Suspended``.

A ``PutBucketVersioning`` request that changes ``MfaDelete`` needs a pin as
well, whether it turns MFA delete on or off.

The request carries the token's serial and the pin in the ``x-amz-mfa``
header, separated by a space::

   x-amz-mfa: <serial> <pin>

The serial must be that of a TOTP token that belongs to the user who signs the
request. A request that needs a pin fails with ``403 AccessDenied`` when the
header is missing or malformed, when the serial is not one of that user's
tokens, or when the pin is wrong or has already been used.

RGW does not require HTTPS for these requests. The header carries the serial
and the pin in the clear, however, so send them over HTTPS.


How pins are checked
====================

RGW checks a pin against the token's seed with the HMAC-SHA1 algorithm of
RFC 6238, using the clock of the OSD that stores the user's tokens. It accepts
the pin for the current time step, or for up to ``--totp-window`` steps before
or after it. With the default 30-second step and a window of 2, that allows
about one minute of clock difference either way. Pins of six or eight digits
both work, because the length of the pin sets the number of digits checked.

Each pin works only once. After a pin is accepted, by an S3 request or by
``radosgw-admin mfa check``, RGW refuses that pin and every earlier one, so
wait for the next pin before you use the token again. RGW also refuses a
token's sixth check within one time step.


Example: MFA delete on a versioned bucket
=========================================

This example gives the user ``testuser`` a TOTP token, enables versioning and
MFA delete on the bucket ``mybucket``, and then deletes an object version. The
``aws`` commands run with ``testuser``'s S3 credentials.

Generate a seed
---------------

Generate a random secret seed. RFC 4226 recommends 160 bits, and 20 random
bytes encode to 32 base32 characters:

.. prompt:: bash $

   head -c 20 /dev/urandom | base32

The examples below use ``GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ``, the base32 form
of the RFC 6238 test seed. Generate your own seed for a real token.

Create the token
----------------

Create the token for the user. The serial can be any string without spaces,
because the ``x-amz-mfa`` header separates the serial from the pin with a
space. ``--totp-seed-type`` defaults to ``hex``, so pass ``base32`` for a
base32 seed:

.. prompt:: bash #

   radosgw-admin mfa create --uid=testuser --totp-serial=testuser-totp \
                            --totp-seed=GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ \
                            --totp-seed-type=base32

For a hardware token that comes with a seed, use the serial printed on the
token and the seed from its vendor, and set ``--totp-seed-type`` to match the
seed's encoding. If the token uses a time step other than 30 seconds, set
``--totp-seconds`` to match.

``radosgw-admin mfa get`` shows the token's settings:

.. prompt:: bash #

   radosgw-admin mfa get --uid=testuser --totp-serial=testuser-totp

::

   {
       "entry": {
           "type": 2,
           "id": "testuser-totp",
           "seed": "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
           "seed_type": "base32",
           "time_ofs": 0,
           "step_size": 30,
           "window": 2
       }
   }

RGW stores the seed as given, unencrypted, in the zone's OTP pool, for example
``default.rgw.otp``. ``mfa get`` and ``mfa list`` print it, so anyone who can
run ``radosgw-admin`` or read that pool can generate pins.

Enroll the seed in an authenticator
-----------------------------------

Add the seed to any RFC 6238 TOTP app. Either enter the seed as the key of a
time-based token, or give the app an ``otpauth://`` URI, usually as a QR
code::

   otpauth://totp/testuser?secret=GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ&issuer=RGW

Apps assume HMAC-SHA1, six digits, and a 30-second period unless the URI says
otherwise. RGW checks pins only with HMAC-SHA1, so do not set ``algorithm`` to
anything else. If you set ``--totp-seconds``, add a matching ``period``
parameter. The URI holds the seed, so make the QR code with a tool that runs
locally, not with a web service.

Check the token
---------------

Pass the pin that the app shows to ``radosgw-admin mfa check``, which prints
``ok`` when RGW accepts the pin:

.. prompt:: bash #

   radosgw-admin mfa check --uid=testuser --totp-serial=testuser-totp \
                           --totp-pin=<pin>

The check uses up the pin, so wait for the next pin before the next step.

Enable versioning and MFA delete
--------------------------------

Enable versioning and MFA delete in one ``PutBucketVersioning`` request.
Enabling MFA delete itself needs a pin:

.. prompt:: bash $

   aws --endpoint-url https://rgw.example.com s3api put-bucket-versioning \
       --bucket mybucket \
       --versioning-configuration Status=Enabled,MFADelete=Enabled \
       --mfa "testuser-totp <pin>"

Afterward, ``aws s3api get-bucket-versioning`` shows ``MFADelete`` as
``Enabled``, and ``radosgw-admin bucket stats --bucket=mybucket`` shows
``"mfa_enabled": true``.

Delete an object version
------------------------

Deleting an object version now needs a pin. Find the version ID with
``aws s3api list-object-versions``, then delete that version:

.. prompt:: bash $

   aws --endpoint-url https://rgw.example.com s3api delete-object \
       --bucket mybucket --key myobject --version-id <version-id> \
       --mfa "testuser-totp <pin>"

Without ``--mfa``, or with a pin that has already been used, the request fails
with ``AccessDenied``. ``aws s3api delete-objects`` takes the same ``--mfa``
option. To turn MFA delete off, send ``MFADelete=Disabled`` with a pin in the
same way.

Before you remove a user's token with ``radosgw-admin mfa remove``, turn off
MFA delete on the buckets that rely on it. A user with no token cannot delete
object versions or change versioning on those buckets through S3.

Re-sync a token whose clock has drifted
---------------------------------------

If RGW refuses the pins that the token shows because the token's clock has
drifted, read two consecutive pins from the token and pass them to
``radosgw-admin mfa resync``, the earlier pin first:

.. prompt:: bash #

   radosgw-admin mfa resync --uid=testuser --totp-serial=testuser-totp \
                            --totp-pin=<pin> --totp-pin=<next-pin>

``radosgw-admin`` looks for the pair within seven days either side of the
OSD's clock, and stores the offset that it finds as ``time_ofs``.
