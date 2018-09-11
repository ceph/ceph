.. _rgw_mfa:

==========================================
RGW Support for Multifactor Authentication
==========================================

.. versionadded:: Mimic

The S3 multifactor authentication (MFA) feature allows
users to require the use of one-time password when removing
objects on certain buckets. The buckets need to be configured
with versioning and MFA enabled which can be done through
the S3 api.

Time-based one time password tokens can be assigned to a user
through radosgw-admin. Each token has a secret seed, and a serial
id that is assigned to it. Tokens are added to the user, can
be listedm removed, and can also be re-synchronized.

Multisite
=========

While the MFA IDs are set on the user's metadata, the
actual MFA one time password configuration resides in the local zone's
osds. Therefore, in a multi-site environment it is advisable to use
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
                              --totp-pin=<prev-pin> --totp=pin=<current-pin>


