.. _radosgw-ext-authz:

========================================================
STS External Authorization for AssumeRoleWithWebIdentity
========================================================

RGW can ask an external service whether a caller may assume a role, in
addition to the checks RGW already makes. The check runs in
``AssumeRoleWithWebIdentity`` after the web identity token has been validated
and the role's trust policy has allowed the request, and before credentials
are issued.

The external authorizer can only add a restriction. If the token or the trust
policy already deny the request, the authorizer is not consulted.

Behavior
========

When the authorizer is enabled, credentials are issued only if the service
returns an explicit Allow. The request is denied (HTTP 403) when the service:

* returns Deny,
* does not answer within ``request_timeout_secs``,
* cannot be reached, or returns an HTTP error,
* returns a response that is not a single well formed decision,
* or when the stored configuration is invalid or cannot be read.

Decisions are not cached. Only the access token that authenticates RGW to the
service is cached, until shortly before it expires. If the service rejects a
cached token with HTTP 401 (for example because it was revoked early), RGW
fetches a new token and repeats the request once. No other failure is retried.
TLS certificates are always verified.

When no configuration is set, or ``enabled`` is false, STS behaves as before
for roles that do not name an authorizer.

Requiring an authorizer for a role
----------------------------------

Tag a role with ``rgw:ext-authz`` and the name of the provider to bind it to
that authorizer::

   aws iam tag-role --role-name ibmreadonly \
       --tags Key=rgw:ext-authz,Value=ibm-iam

The role can then be assumed only while the configured, enabled authorizer is
that provider. Otherwise AssumeRoleWithWebIdentity is denied, so removing or
disabling the authorizer, or setting a different provider, does not leave the
role open to whatever its trust policy allows. The request is also denied when
the tag has an empty value or several different values. A role without the tag
is unaffected. The tag key is matched exactly, so a misspelled key is the same
as no tag.

The tag is control metadata for RGW. Unlike other role tags it is not copied
into the session as ``aws:PrincipalTag/rgw:ext-authz``, so it does not make
RGW require ``sts:TagSession`` in the trust policy and policies cannot match
on it as an identity attribute. It is still visible to policies as the
resource tag ``iam:ResourceTag/rgw:ext-authz``. Any other tag on the role, and
any tag carried in the web identity token, is handled as before.

Anyone allowed to tag the role can also remove the tag, so restrict
``iam:TagRole`` and ``iam:UntagRole`` to administrators.

Providers
=========

``ibm-iam``
-----------

Uses IBM IAM. RGW obtains an access token with an API key
(``POST {endpoint}/identity/token``) and asks the policy enforcement service
(``POST {pep_endpoint or endpoint}/v2/authz``) with the caller's web identity
token as the subject. Only the payload (middle) segment of the JWT is sent as
``subject.accessTokenBody``, which is what IBM IAM accepts; the signed token
never leaves RGW.

The action is chosen from the role RGW resolved, never from caller input. The
action names are configurable (``action_readonly``, ``action_readwrite`` and
``action_assume_role``) and default to:

* the role named ``readonly_role_name``: ``software-defined-storage.s3.readonly``
* the role named ``readwrite_role_name``: ``software-defined-storage.s3.readwrite``
* any other role: ``software-defined-storage.s3.assume-role``

The three actions must be non-empty and different from each other. If they
could be equal, an Allow for the read-only action would also authorize assuming
a read/write or custom role.

The resource carries the service instance and account from ``service_instance``
(a CRN), the Object Account ID that owns the role (``objectAccount``) and, for
``assume-role``, the role ARN (``objectRole``). The RGW transaction id is sent
as ``transaction-id`` so that a decision can be correlated with the service's
logs.

Configuration
=============

.. code-block:: json

   {
     "enabled": true,
     "provider": "ibm-iam",
     "endpoint": "https://iam.cloud.ibm.com",
     "pep_endpoint": "",
     "service_credential": "<api key>",
     "service_credential_file": "",
     "service_instance": "crn:v1:bluemix:public:software-defined-storage:us-south:a/<account>:<guid>::",
     "tls_ca_bundle": "",
     "connect_timeout_secs": 2,
     "request_timeout_secs": 5,
     "readonly_role_name": "",
     "readwrite_role_name": "",
     "action_readonly": "software-defined-storage.s3.readonly",
     "action_readwrite": "software-defined-storage.s3.readwrite",
     "action_assume_role": "software-defined-storage.s3.assume-role"
   }

``endpoint`` and ``pep_endpoint`` must be ``https://`` URLs. Timeouts must be
1 to 60 seconds. ``tls_ca_bundle`` holds PEM text and replaces the system trust store for
the service calls. Leave it empty for a service with a publicly trusted
certificate. With libcurl older than 7.77 it needs the OpenSSL backend. Set either ``service_credential`` or ``service_credential_file``. If the
file is set it is read on use and wins, so the key can be provisioned per site
and rotated without a config change. A site that needs a different key from
the replicated one sets its own file path locally.

Manage it with ``radosgw-admin``:

.. prompt:: bash #

   radosgw-admin ext-authz set --infile config.json
   radosgw-admin ext-authz get
   radosgw-admin ext-authz rm

or with the Admin Ops API, which needs the ``ext-authz`` capability (read for
``GET``, write for ``PUT`` and ``DELETE``):

.. code-block:: none

   GET    /admin/ext-authz
   PUT    /admin/ext-authz     (body: the json above)
   DELETE /admin/ext-authz

The credential is never printed. ``get`` shows ``<redacted>``, and a ``set``
whose ``service_credential`` is ``<redacted>`` keeps the stored one, so the
output of ``get`` can be edited and submitted again.

Multisite
=========

The configuration is stored in the zone's OIDC pool and replicates to all
zones through metadata sync (section ``ext_authz``, key ``default``). Changes
made on a non-master zone are forwarded to the metadata master.

The inline ``service_credential`` is handled like other RGW secrets, such as
user access keys and the zone system key: it is stored unencrypted in the pool,
fetched by the other zones over their zone endpoints (use ``https://``
endpoints), and readable through ``radosgw-admin metadata get`` by users with
the ``metadata`` capability. To keep a key out of the cluster, or to give each
site its own key, set ``service_credential_file`` instead. It is a local path
and takes precedence over the inline key.

Monitoring
==========

Each decision logs one ``ext_authz audit`` line at debug level 1 with the
outcome, transaction id, subject, issuer, account, role, action and latency.
It never contains the credential or the token.

The ``rgw`` perf counters ``ext_authz_req``, ``ext_authz_allow``,
``ext_authz_deny``, ``ext_authz_error`` and ``ext_authz_lat`` count requests,
decisions, failures (including timeouts) and the time spent in the service.
