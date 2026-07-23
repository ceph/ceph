=============
 STS in Ceph
=============

Secure Token Service is a web service in AWS that returns a set of temporary security credentials for authenticating federated users.
The link to official AWS documentation can be found here: https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html.

Ceph Object Gateway implements a subset of STS APIs that provide temporary credentials for identity and access management.
These temporary credentials can be used to make subsequent S3 calls which will be authenticated by the STS engine in Ceph Object Gateway.
Permissions of the temporary credentials can be further restricted via an IAM policy passed as a parameter to the STS APIs.

STS REST APIs
=============

The following STS REST APIs have been implemented in Ceph Object Gateway:

#. AssumeRole: Returns a set of temporary credentials that can be used for 
   cross-account access. The temporary credentials will have permissions that are
   allowed by both - permission policies attached with the Role and policy attached
   with the AssumeRole API.

   Parameters:
    **RoleArn** (String/ Required): ARN of the Role to Assume.

    **RoleSessionName** (String/ Required): An Identifier for the assumed role
    session.

    **Policy** (String/ Optional): An IAM Policy in JSON format.

    **DurationSeconds** (Integer/ Optional): The duration in seconds of the session.
    Its default value is 3600.

    **ExternalId** (String/ Optional): A unique ID that might be used when a role is
    assumed in another account.

    **SerialNumber** (String/ Optional): The ID number of the MFA device associated
    with the user making the AssumeRole call.

    **TokenCode** (String/ Optional): The value provided by the MFA device, if the
    trust policy of the role being assumed requires MFA.

#. AssumeRoleWithWebIdentity: Returns a set of temporary credentials for users that
   have been authenticated by a web/mobile app by an OpenID Connect/OAuth 2.0 Identity Provider.
   Currently Keycloak has been tested and integrated with RGW.

   Parameters:
    **RoleArn** (String/ Required): ARN of the Role to Assume.

    **RoleSessionName** (String/ Required): An Identifier for the assumed role
    session.

    **Policy** (String/ Optional): An IAM Policy in JSON format.

    **DurationSeconds** (Integer/ Optional): The duration in seconds of the session.
    Its default value is 3600.

    **ProviderId** (String/ Optional): Fully qualified host component of the domain name
    of the IDP. Valid only for OAuth 2.0 tokens (not for OpenID Connect tokens).

    **WebIdentityToken** (String/ Required): The OpenID Connect/OAuth 2.0 token, which the
    application gets in return after authenticating its user with an IDP.

#. GetCallerIdentity: Returns details about the IAM user or role whose credentials are used to call the operation.

   Response:
    **Account** The account ID that owns or contains the calling entity.

    **Arn** The ARN associated with the calling entity.

    **UserId** The unique identifier of the calling entity (user or assumed role).

.. note:: No permissions are required to perform GetCallerIdentity.

Before invoking AssumeRoleWithWebIdentity, an OpenID Connect Provider entity (which the web application
authenticates with), needs to be created in RGW.

The trust between the IDP and the role is created by adding a condition to the role's trust policy, which
allows access only to applications which satisfy the given condition.
All claims of the JWT are supported in the condition of the role's trust policy.
An example of a policy that uses the 'aud' claim in the condition is of the form::

    '''{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":["arn:aws:iam:::oidc-provider/<URL of IDP>"]},"Action":["sts:AssumeRoleWithWebIdentity"],"Condition":{"StringEquals":{"<URL of IDP> :app_id":"<aud>"}}}]}'''

The ``app_id`` in the condition above must match the 'aud' claim of the incoming token.

An example of a policy that uses the 'sub' claim in the condition is of the form::

    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/<URL of IDP>\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"<URL of IDP> :sub\":\"<sub>\"\}\}\}\]\}"

Similarly, an example of a policy that uses 'azp' claim in the condition is of the form::

    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/<URL of IDP>\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"<URL of IDP> :azp\":\"<azp>\"\}\}\}\]\}"

A shadow user is created corresponding to every federated user. The user ID is derived from the ``sub`` field of the incoming web token.
The user is created in a separate namespace ``oidc`` such that the user ID doesn't clash with any other user IDs in RGW. The format of the user ID
is ``<tenant>$<user-namespace>$<sub>`` where ``user-namespace`` is ``oidc`` for users that authenticate with OIDC providers.

RGW now supports Session tags that can be passed in the web token to AssumeRoleWithWebIdentity call. More information related to Session Tags can be found here
:doc:`session-tags`.

STS Configuration
=================

The following configurable options have to be added for STS integration:

.. confval:: rgw_sts_key
.. confval:: rgw_sts_token_format
.. confval:: rgw_sts_keyring_refresh_interval
.. confval:: rgw_sts_max_session_duration
.. confval:: rgw_s3_auth_use_sts

AES-256-GCM session-token sealing
---------------------------------

The keyring that seals and verifies AES-256-GCM session tokens is stored in
the Monitor config-key store under ``rgw/sts/keys``, alongside other cluster
secrets, and is managed with ``radosgw-admin sts keyring`` commands. The
keyring seals only ``aead`` format tokens; the ``legacy`` format uses a
single key, managed with the same commands under ``--legacy`` and always
overridden by :confval:`rgw_sts_key` when that option is set (see `Storing
the legacy key in the config-key store`_). The stored value is a
whitespace-separated list of ``<key-id>=<key>`` entries,
where the key id is 40 hexadecimal characters (20 bytes) and the key is the
canonical padded base64 encoding of 32 random bytes. At most 16 entries are
accepted. The first entry seals new tokens; every entry verifies.

Each RGW reads the keyring through its Monitor session on a background
thread and caches an immutable snapshot, so token operations never wait on
the Monitor. The snapshot is refreshed every
:confval:`rgw_sts_keyring_refresh_interval` seconds (one minute by default),
so a rotated key propagates within one interval.

Monitor access
~~~~~~~~~~~~~~

An RGW reads the keyring and the stored legacy key with the
``config-key get`` Monitor command. ``mon 'profile rgw'`` grants that under
the ``rgw/`` prefix, as does ``mon 'allow *'``. Blanket capabilities such as
``mon 'allow rw'`` do not. ``ceph auth caps`` replaces a key's entire
capability set, so give all three:

.. prompt:: bash $

   ceph auth caps client.rgw.<id> mon 'profile rgw' osd 'profile rgw' mgr 'profile rgw'

The keyring travels over each RGW's Monitor session. An RGW logs a
warning when that session permits insecure connection modes; prefer msgr2
secure mode on deployments that use token sealing.

New deployments
~~~~~~~~~~~~~~~

For a single cluster with no existing STS credentials, create the keyring
and enable AEAD sealing:

.. prompt:: bash $

   radosgw-admin sts keyring init
   ceph config set client.rgw rgw_sts_token_format aead

``sts keyring init`` generates one random key and prints its id. It refuses
to overwrite an existing keyring. :confval:`rgw_sts_token_format` is
runtime-updatable and does not require a restart. Verify the effective
settings on every daemon under `Verifying the running configuration`_ and
complete `End-to-end token verification`_ before admitting production
traffic.

Manually generated keys and multisite
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every cluster whose RGWs issue or verify the same tokens must hold an
identical keyring, including all zones in the realm and all federating
realms: each cluster's config-key store is independent, and a cluster cannot
verify tokens sealed with a key its own store does not hold. Generate the
keyring once and install the same file on every cluster instead of running
a bare ``init`` per cluster. The key file is a secret; create it under
``umask 077`` so it is readable only by its owner:

.. prompt:: bash $

   umask 077
   echo "$(openssl rand -hex 20)=$(openssl rand -base64 32)" > sts-keyring.txt
   radosgw-admin sts keyring init --infile=sts-keyring.txt

``--infile`` accepts the same entry format as the stored keyring and applies
the same strict validation (canonical base64, unique ids and key material,
at most 16 entries), so a malformed file is rejected before it reaches the
Monitors. To export the keyring of a cluster that used generated keys, for
example when another cluster joins the deployment later:

.. prompt:: bash $

   umask 077
   ceph config-key get rgw/sts/keys > sts-keyring.txt

Storing the legacy key in the config-key store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``legacy`` token key can be stored at ``rgw/sts/legacy_key`` beside the
AEAD keyring instead of in the configuration database, where any client
with ordinary Monitor capabilities can read it. The same commands manage
it under ``--legacy``:

.. prompt:: bash $

   radosgw-admin sts keyring init --legacy
   radosgw-admin sts keyring list --legacy

``init --legacy`` generates a 16-character key, or installs one supplied
with ``--infile``; with ``--yes-i-really-mean-it`` it replaces a stored key
in one step, which invalidates all outstanding legacy tokens.
``list --legacy`` prints a sha256 digest of the stored key and never the
key itself; compare digests across clusters the way keyrings are compared
under `Manually generated keys and multisite`_. ``rm --legacy`` removes
the stored key. ``rotate`` and ``trim`` do not apply: legacy tokens carry
no key id, so only one legacy key can exist.

When :confval:`rgw_sts_key` is set it always takes precedence and the
stored key is ignored; RGW logs a warning when the two differ. To
migrate an existing deployment, store the current key with
``init --legacy --infile``, confirm the digest on every cluster, and then
unset ``rgw_sts_key`` everywhere, including any daemon-local configuration
files. Each RGW adopts the stored key within one
:confval:`rgw_sts_keyring_refresh_interval`. In multisite deployments,
install the same value on every cluster, as with the AEAD keyring.

Upgrading an existing deployment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default ``legacy`` mode issues AES-128-CBC tokens and accepts both legacy
and AES-256-GCM tokens. The ``aead`` mode issues and accepts only AES-256-GCM
tokens. Use this ordering when upgrading:

#. Upgrade every RGW that issues or verifies the credentials, in every zone
   and federating realm. Keep :confval:`rgw_sts_token_format` at ``legacy`` and
   use the deployment or orchestrator inventory to confirm that no old RGW,
   including a stopped daemon that could later rejoin, remains. Use
   ``ceph versions`` in each cluster to verify the versions of running daemons.
   Old binaries cannot verify AES-256-GCM tokens.
#. Install the same keyring in every cluster while all RGWs remain in
   ``legacy`` mode, as described in `Manually generated keys and
   multisite`_:

   .. prompt:: bash $

      radosgw-admin sts keyring init --infile=sts-keyring.txt

#. Verify the keyring in every cluster and confirm that every RGW still
   uses ``legacy`` mode. Resolve any daemon-specific, local-file,
   command-line, or runtime overrides of the format before continuing.
#. Test the keyring before the fleet-wide cutover. Drain one upgraded RGW
   from normal traffic, set a daemon-specific override, and verify that it uses
   ``aead`` mode:

   .. prompt:: bash $

      canary='client.rgw.<canary-daemon-id>'
      ceph config set "$canary" rgw_sts_token_format aead
      ceph config show "$canary" rgw_sts_token_format

   Complete `End-to-end token verification`_ with credentials issued directly
   through the canary's STS endpoint. Return the canary to the inherited
   ``legacy`` setting and verify it before restoring normal traffic:

   .. prompt:: bash $

      ceph config rm "$canary" rgw_sts_token_format
      ceph config show "$canary" rgw_sts_token_format

#. In a coordinated maintenance change, enable AEAD in every cluster as close
   together as possible:

   .. prompt:: bash $

      ceph config set client.rgw rgw_sts_token_format aead

   Runtime configuration propagates asynchronously, so this cutover is not
   atomic across daemons or clusters. During a partial cutover, AEAD-mode
   RGWs reject legacy tokens that legacy-mode RGWs can still issue.
   Existing legacy sessions must re-authenticate.
#. Verify ``aead`` mode everywhere, then complete `End-to-end token
   verification`_.
#. Retain :confval:`rgw_sts_key` until the rollback window has closed. When it
   is no longer needed, remove it from each scope where it was configured. For
   example, if it was set at the ``client.rgw`` scope:

   .. prompt:: bash $

      ceph config rm client.rgw rgw_sts_key

Verifying the running configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``radosgw-admin sts keyring list`` prints key ids and never key material,
and it parses the stored keyring with the same checks the daemon applies,
so it doubles as a correctness check after a manual installation. To compare keyrings across clusters,
compare digests instead of printing the value:

.. prompt:: bash $

   radosgw-admin sts keyring list
   ceph config-key get rgw/sts/keys | sha256sum
   ceph config show client.rgw.<daemon-id> rgw_sts_token_format

The digest must match on every cluster, and the format must have the value
required by the current migration step on every RGW. The raw
``ceph config-key get`` output contains the secret keyring, so digest it
rather than copying it to logs or tickets.

End-to-end token verification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Issue temporary security credentials through every STS endpoint that is
expected to issue them, then use each returned session token for an S3 request
through every RGW endpoint that is expected to verify it. This confirms
token issuance and cross-daemon verification with the effective runtime
configuration.

Waiting out issued tokens
~~~~~~~~~~~~~~~~~~~~~~~~~

Rollback and key retirement wait for previously issued tokens to expire. Role
credentials can currently last up to 43,200 seconds;
:confval:`rgw_sts_max_session_duration` governs only ``GetSessionToken`` and is
not by itself the wait bound.

Rolling back to legacy sealing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rollback must finish before any RGW is downgraded to a binary that does not
support AES-256-GCM tokens:

#. Ensure that the same original :confval:`rgw_sts_key` is still available to
   every RGW. This option is not runtime-updatable. If it was removed, set
   it again in every cluster, restart every RGW with the deployment's normal
   restart procedure, and verify the effective value before continuing. For
   example, compare digests without printing the key:

   .. prompt:: bash $

      rgw_daemon='client.rgw.<daemon-id>'
      legacy_key='<original-legacy-key>'
      ceph config set client.rgw rgw_sts_key "$legacy_key"
      printf '%s\n' "$legacy_key" | sha256sum
      ceph config show "$rgw_daemon" rgw_sts_key | sha256sum

#. Set every upgraded RGW back to ``legacy`` mode in a coordinated change:

   .. prompt:: bash $

      ceph config set client.rgw rgw_sts_token_format legacy

   Upgraded RGWs now issue legacy tokens but continue to accept both token
   formats.
#. Verify ``legacy`` mode on every daemon. Keep the AEAD keyring and the
   upgraded binaries in place for the maximum lifetime of any AES-256-GCM token
   issued before the last daemon was confirmed in ``legacy`` mode, as described
   in `Waiting out issued tokens`_.
#. After all AES-256-GCM tokens have expired, old RGW binaries may be
   installed. The keyring can then be removed:

   .. prompt:: bash $

      ceph config-key rm rgw/sts/keys

Rotating AEAD keys
~~~~~~~~~~~~~~~~~~

``sts keyring rotate`` prepends a new key, which seals all new tokens, and
preserves every existing entry for verification. Each RGW adopts the new
key on its next scheduled refresh, so during a rotation an RGW that has
already refreshed may seal a token with the new key that an RGW which has
not refreshed cannot yet verify. This window closes within one
:confval:`rgw_sts_keyring_refresh_interval`; the previous key stays valid
throughout, so already-issued sessions are unaffected.

.. note:: The keyring commands (``init``, ``rotate``, ``rm``, and ``trim``)
   read the stored keyring, modify it, and write it back. The config-key store
   has no compare-and-swap, so two commands running against the same cluster at
   once can lose one of their changes. Run keyring commands serially; do not
   invoke them concurrently on the same cluster.

On a single cluster, rotate with a generated key:

.. prompt:: bash $

   radosgw-admin sts keyring rotate

In a multisite deployment, generate the new key once and rotate every
cluster with the same entry:

.. prompt:: bash $

   umask 077
   echo "$(openssl rand -hex 20)=$(openssl rand -base64 32)" > new-key.txt
   radosgw-admin sts keyring rotate --infile=new-key.txt

Run the rotation on all clusters promptly, ideally within one keyring
refresh interval (one minute). A cluster can only learn keys from its own
config-key store, so until its rotation lands, it rejects tokens that
another cluster already seals with the new key. Sealing adopts a new key
only on a cache refresh, so completing all rotations within the interval
avoids this window in practice.

The keyring holds at most 16 entries. ``rotate --max-keys=<count>``
bounds its size in the same step by dropping the oldest entries beyond
``<count>`` after prepending the new key; use a bound large enough to keep
every key that may still have live tokens. ``rotate --max-keys=1`` discards
the current sealing key, invalidating all outstanding tokens, and requires
``--yes-i-really-mean-it``. If the keyring is already full,
first retire a verification key whose tokens have all expired. Never remove
a key that may still have live tokens merely to make room.

An RGW that has not refreshed its cached keyring yet keeps sealing with
the old key for up to one refresh interval (one minute) after its cluster's
rotation. Starting from the last cluster's rotation plus that interval, wait
for the maximum lifetime of any token issued under the old key, as described
in `Waiting out issued tokens`_. Then retire the oldest key everywhere:

.. prompt:: bash $

   radosgw-admin sts keyring trim

``trim`` removes the oldest entry and never the sealing key;
``trim --max-keys=<count>`` instead drops as many of the oldest entries
as needed to leave at most ``<count>``, and ``rm --key-id=<id>`` removes one
specific key.

Removing a key invalidates tokens that it sealed. If a key is exposed,
remove it as soon as the replacement has propagated instead of retaining it
for the normal wait period; every RGW that still trusts the exposed key
remains forgeable.

.. warning:: The keys in ``rgw/sts/keys`` authenticate every AEAD session
   token: anyone who obtains one can forge a valid token for any user or role,
   just as a leaked ``rgw_sts_key`` allows forging legacy tokens. The
   config-key store is not readable through blanket Monitor capabilities, and
   ``radosgw-admin sts keyring list`` never prints key material. Keep exported
   keyring files out of plaintext repositories and logs, and distribute them
   to every zone and realm over a secure channel.

.. note:: The STS and S3 APIs co-exist in the same namespace, and both S3
   and STS APIs can be accessed via the same endpoint.

Examples
========

#. In order to get the example to work, make sure that the user ``TESTER`` has the ``roles`` capability assigned:

   .. prompt:: bash #

    radosgw-admin caps add --uid="TESTER" --caps="roles=*"

#. The following is an example of the AssumeRole API call, which shows steps to create a role, assign a policy to it
   (that allows access to S3 resources), assuming a role to get temporary credentials and accessing S3 resources using
   those credentials. In this example, ``TESTER1`` assumes a role created by ``TESTER``, to access S3 resources owned by ``TESTER``,
   according to the permission policy attached to the role.

   .. code-block:: python

    import boto3

    iam_client = boto3.client('iam',
    aws_access_key_id=<access_key of TESTER>,
    aws_secret_access_key=<secret_key of TESTER>,
    endpoint_url=<IAM URL>,
    region_name=''
    )

    policy_document = '''{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/TESTER1"]},"Action":["sts:AssumeRole"]}]}'''

    role_response = iam_client.create_role(
    AssumeRolePolicyDocument=policy_document,
    Path='/',
    RoleName='S3Access',
    )

    role_policy = '''{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}}'''

    response = iam_client.put_role_policy(
    RoleName='S3Access',
    PolicyName='Policy1',
    PolicyDocument=role_policy
    )

    sts_client = boto3.client('sts',
    aws_access_key_id=<access_key of TESTER1>,
    aws_secret_access_key=<secret_key of TESTER1>,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = sts_client.assume_role(
    RoleArn=role_response['Role']['Arn'],
    RoleSessionName='Bob',
    DurationSeconds=3600
    )

    s3client = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url=<S3 URL>,
    region_name='',)

    bucket_name = 'my-bucket'
    s3bucket = s3client.create_bucket(Bucket=bucket_name)
    resp = s3client.list_buckets()

#. The following is an example of AssumeRoleWithWebIdentity API call, where an external app that has users authenticated with
   an OpenID Connect/OAuth 2.0 IDP (Keycloak in this example), assumes a role to get back temporary credentials and access S3 resources
   according to permission policy of the role.

   .. code-block:: python

    import boto3

    iam_client = boto3.client('iam',
    aws_access_key_id=<access_key of TESTER>,
    aws_secret_access_key=<secret_key of TESTER>,
    endpoint_url=<IAM URL>,
    region_name=''
    )

    oidc_response = iam_client.create_open_id_connect_provider(
        Url=<URL of the OpenID Connect Provider>,
        ClientIDList=[
            <Client id registered with the IDP>
        ],
        ThumbprintList=[
            <Thumbprint of the IDP>
     ]
    )

    policy_document = '''{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/demo"]},"Action":["sts:AssumeRoleWithWebIdentity"],"Condition":{"StringEquals":{"localhost:8080/auth/realms/demo:app_id":"customer-portal"}}}]}'''
    role_response = iam_client.create_role(
    AssumeRolePolicyDocument=policy_document,
    Path='/',
    RoleName='S3Access',
    )

    role_policy = '''{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}}'''

    response = iam_client.put_role_policy(
        RoleName='S3Access',
        PolicyName='Policy1',
        PolicyDocument=role_policy
    )

    sts_client = boto3.client('sts',
    aws_access_key_id=<access_key of TESTER1>,
    aws_secret_access_key=<secret_key of TESTER1>,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = sts_client.assume_role_with_web_identity(
    RoleArn=role_response['Role']['Arn'],
    RoleSessionName='Bob',
    DurationSeconds=3600,
    WebIdentityToken=<Web Token>
    )

    s3client = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url=<S3 URL>,
    region_name='',)

    bucket_name = 'my-bucket'
    s3bucket = s3client.create_bucket(Bucket=bucket_name)
    resp = s3client.list_buckets()


#. The following is an example of GetCallerIdentity API call assuming a role, which shows steps to create a role, 
   assuming a role to get temporary credentials and getting caller identity using those credentials.

   .. code-block:: python

    import boto3
    import json

    USER_ID = 'tester'
    ACCESS_KEY = 'TESTER'
    SECRET_KEY = 'test123'
    ENDPOINT_URL = 'http://localhost:8000'
    REGION = 'us-east-1'

    ROLE_NAME = 'S3Access'
    ROLE_SESSION_NAME = 'Bob'
    DURATION_SECONDS = 3600

    iam_client = boto3.client('iam',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION
    )

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": [f"arn:aws:iam:::user/{USER_ID}"] },
            "Action": ["sts:AssumeRole"]
        }]
    })

    role_response = iam_client.create_role(
        RoleName=ROLE_NAME,
        Path='/xxx/policy/',
        AssumeRolePolicyDocument=trust_policy
    )

    sts_client = boto3.client('sts',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION
    )

    response = sts_client.assume_role(
        RoleArn=role_response['Role']['Arn'],
        RoleSessionName=ROLE_SESSION_NAME,
        DurationSeconds=DURATION_SECONDS
    )
    creds = response['Credentials']

    session_sts = boto3.client('sts',
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
        endpoint_url=ENDPOINT_URL,
        region_name=REGION
    )
    identity = session_sts.get_caller_identity()

#. The following is an example of GetCallerIdentity API call with user credentials

  .. code-block:: python

    import boto3

    sts = boto3.client('sts',
        aws_access_key_id=<access_key>,
        aws_secret_access_key=<secret_key>,
        endpoint_url='http://localhost:8000',
        region_name='us-east-1'
    )

    identity = sts.get_caller_identity()

How to obtain thumbprint of an OpenID Connect Provider IDP
==========================================================

#. Take the OpenID connect provider's URL and add ``/.well-known/openid-configuration``
   to it to get the URL to get the IDP's configuration document. For example, if the URL
   of the IDP is http://localhost:8000/auth/realms/quickstart, then the URL to get the
   document from is http://localhost:8000/auth/realms/quickstart/.well-known/openid-configuration

#. Use the following curl command to get the configuration document from the URL described
   in step 1:

   .. prompt:: bash $

      curl -k -v \
        -X GET \
        -H "Content-Type: application/x-www-form-urlencoded" \
        "http://localhost:8000/auth/realms/quickstart/.well-known/openid-configuration" \
      | jq .

#. From the response of step 2, use the value of "jwks_uri" to get the certificate of the IDP,
   using the following code:

   .. prompt:: bash $

      curl -k -v \
       -X GET \
       -H "Content-Type: application/x-www-form-urlencoded" \
       "http://$KC_SERVER/$KC_CONTEXT/realms/$KC_REALM/protocol/openid-connect/certs" \
      | jq .

#. Copy the result of ``x5c`` in the response above, in a file ``certificate.crt``, and add
   ``-----BEGIN CERTIFICATE-----`` at the beginning and ``-----END CERTIFICATE-----``
   at the end.

#. Use the following OpenSSL command to get the certificate thumbprint:

   .. prompt:: bash $

      openssl x509 -in certificate.crt -fingerprint -noout

#. The result of the above command in step 5, will be a SHA1 fingerprint, like the following::

    SHA1 Fingerprint=F7:D7:B3:51:5D:D0:D3:19:DD:21:9A:43:A9:EA:72:7A:D6:06:52:87

#. Remove the colons from the result above to get the final thumbprint which can be as input
   while creating the OpenID Connect Provider entity in IAM::

    F7D7B3515DD0D319DD219A43A9EA727AD6065287

Roles in RGW
============

More information for role manipulation can be found here
:doc:`role`.

OpenID Connect Provider in RGW
==============================

More information for OpenID Connect Provider entity manipulation
can be found here
:doc:`oidc`.

Keycloak integration with RGW
=============================

Steps for integrating RGW with Keycloak can be found here
:doc:`keycloak`.

STSLite
=======
STSLite has been built on STS, and documentation for the same can be found here
:doc:`STSLite`.