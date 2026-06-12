.. _mgr-ceph-secrets:

===================
Ceph Secrets Module
===================

The ``ceph_secrets`` manager module provides centralised secret storage for
Ceph operators and Ceph manager modules.  Instead of embedding plaintext
credentials in service specifications or configuration objects, secrets are
stored once and referenced by URI.  Ceph manager modules such as cephadm
and rook can store secret payloads centrally, reference them by URI, and
resolve them to plaintext only when needed at deploy time.

For example, a cephadm-managed service may need an API token.  Instead of
embedding that token directly in the service specification, an operator can
store it once:

.. prompt:: bash $

   ceph secret set cephadm/service/my-service/api_token -i /tmp/api-token

and then use the URI
``secret:/cephadm/service/my-service/api_token`` in the
service spec instead of the plaintext token.  A cephadm integration can then
resolve the URI at deploy time and write the token only into the daemon files
that need it.

Secrets are stored in the Mon KV store under the ``secret_store/v1/`` prefix
and are organised by namespace, scope, and name.  Each secret is versioned and
carries ``created``/``updated`` timestamps.  A per-namespace epoch counter
is incremented on every ``set`` and on any ``rm`` that actually removes a
secret, allowing consumers to detect changes without fetching the full secret
list.

.. note::

   The ``mon`` backend stores secrets in the Mon KV store, which is not an
   external KMS or vault.  Users and MGR modules with sufficient Ceph
   permissions can still reveal or resolve stored secret values.  Namespaces
   provide logical and storage isolation, not an authorisation boundary.

If the module is not already enabled, run::

    ceph mgr module enable ceph_secrets

.. contents::
   :local:
   :depth: 2


Concepts
========

Namespace
---------

A namespace is a logical storage boundary, typically matching the name of
the consuming MGR module (e.g. ``cephadm``, ``rook``).  Secrets in different
namespaces are stored independently and have independent epoch counters.
Namespaces are not an authorisation boundary; any caller with access to the
``ceph_secrets`` module can read secrets from any namespace.

Scope
-----

Within a namespace, every secret has a scope that encodes what the secret
belongs to:

.. list-table::
   :header-rows: 1
   :widths: 15 40 45

   * - Scope
     - Meaning
     - CLI path form
   * - ``global``
     - Cluster-wide secret, not tied to any specific target
     - ``<namespace>/global/<name>``
   * - ``service``
     - Secret for a specific named service
     - ``<namespace>/service/<service-name>/<name>``
   * - ``host``
     - Secret for a specific host
     - ``<namespace>/host/<hostname>/<name>``
   * - ``custom``
     - Arbitrary slash-delimited path under the namespace
     - ``<namespace>/custom/<path>``

Path grammar
------------

All path segments (namespace, scope target, name, and each segment of a
custom path) must match ``[A-Za-z0-9._-]+`` and must not end with ``'.'``.
Empty segments, percent-encoding, and leading/trailing whitespace are
rejected.

Custom scope paths may contain multiple slash-separated segments (e.g.
``cephadm/custom/app/db/password``).  Two custom paths that share a common
prefix are independent secrets; ``cephadm/custom/a/b`` and
``cephadm/custom/a/b/c`` do not conflict.

Secret URIs
-----------

Internally, secrets are identified by URIs of the form::

    secret:/<namespace>/global/<name>
    secret:/<namespace>/service/<target>/<name>
    secret:/<namespace>/host/<target>/<name>
    secret:/<namespace>/custom/<path>

These URIs appear in the output of :ref:`scan_refs <mgr-ceph-secrets-scan>`
and may be embedded in configuration objects as opaque references to be
resolved at deploy time.


CLI Reference
=============

All CLI commands use the path form described above.  Responses are JSON by
default; pass ``--format yaml`` for YAML output.

.. _mgr-ceph-secrets-set:

secret set
----------

Create or update a secret. The input file content is stored as an opaque string::

    ceph secret set <path> -i <file>

Secret data must not be empty.  Other contents, including leading or trailing
whitespace and final newlines, are preserved exactly.  Callers that need to
store structured data should encode it themselves, for example as JSON, and
decode it after retrieval or resolution.

If the secret already exists its data is replaced and its version
incremented, unless the existing secret policy marks it non-editable (set via
the Python API with ``editable=False``), in which case the update is rejected.
The ``created`` timestamp is set on the first write and is never changed
thereafter; ``updated`` is refreshed on every write.

Example:

.. prompt:: bash $

   ceph secret set cephadm/service/my-service/api_token -i /tmp/api-token
   {"metadata": {"version": 1, "created": "2025-06-01T12:00:00Z", "updated": "2025-06-01T12:00:00Z"}}

.. _mgr-ceph-secrets-get:

secret get
----------

Retrieve the metadata (and, optionally, the data) for a secret::

    ceph secret get <path> [--reveal] [--format {json|yaml}]

Without ``--reveal``, the ``data`` field is omitted from the response to
avoid accidental exposure in terminal output or logs.  With ``--reveal``,
``data`` contains the stored opaque string.

Example:

.. prompt:: bash $

   ceph secret get cephadm/service/my-service/api_token
   {
     "metadata": {
       "version": 1,
       "created": "2025-06-01T12:00:00Z",
       "updated": "2025-06-01T12:00:00Z"
     }
   }

   ceph secret get cephadm/service/my-service/api_token --reveal
   {
     "metadata": {
       "version": 1,
       "created": "2025-06-01T12:00:00Z",
       "updated": "2025-06-01T12:00:00Z"
     },
     "data": "api-token-value"
   }

.. _mgr-ceph-secrets-get-value:

secret get-value
----------------

Return the raw secret data string directly, with no JSON envelope::

    ceph secret get-value <path>

Unlike ``secret get --reveal``, the output is the stored string itself,
making it suitable for use in shell scripts and pipelines.  Returns an
error if the secret does not exist.

Example:

.. prompt:: bash $

   ceph secret get-value cephadm/service/my-service/api_token
   api-token-value

.. _mgr-ceph-secrets-ls:

secret ls
---------

List secrets, optionally filtered by namespace, scope, and/or target::

    ceph secret ls [--namespace <ns>] [--scope <scope>]
                   [--sec_target <target>] [--reveal] [--show_internals]
                   [--format {json|yaml}]

Without filters, all secrets across all namespaces are listed.
``--reveal`` includes the stored opaque data string in each output record.
``--show_internals`` additionally includes the ``policy`` object (``user_made``
and ``editable`` flags) in each record.

Example:

.. prompt:: bash $

   ceph secret ls --namespace cephadm --scope host --sec_target node1
   {
     "cephadm/host/node1/ssh_key": {
       "metadata": {
         "version": 3,
         "created": "2025-05-10T08:00:00Z",
         "updated": "2025-06-01T09:00:00Z"
       },
       "ref": {
         "namespace": "cephadm",
         "scope": "host",
         "target": "node1",
         "name": "ssh_key"
       }
     }
   }

.. _mgr-ceph-secrets-rm:

secret rm
---------

Remove a secret::

    ceph secret rm <path>

The operation is idempotent: removing a secret that does not exist succeeds
and reports ``"status": "not_found"`` rather than an error.

Example:

.. prompt:: bash $

   ceph secret rm cephadm/service/my-service/api_token
   {"status": "removed"}

   ceph secret rm cephadm/service/my-service/api_token
   {"status": "not_found"}

.. _mgr-ceph-secrets-get-epoch:

Epoch
-----

The epoch for a namespace is accessible via the Python API only (see
:ref:`Epoch-based change detection <mgr-ceph-secrets-epoch>`).  There is
no CLI command for it.


Module API
==========

Other MGR modules should consume ``ceph_secrets`` via ``CephSecretsClient``
(``src/pybind/mgr/ceph_secrets_client.py``) rather than calling
``mgr.remote()`` directly.  The client file, along with
``ceph_secrets_types.py``, lives at the top level of ``src/pybind/mgr/`` so
any MGR module can import it without depending on the ``ceph_secrets`` package
internals.

.. code-block:: python

    from ceph_secrets_client import CephSecretsClient
    from ceph_secrets_types import SecretScope

    client = CephSecretsClient(self)   # self is a MgrModule instance

    # Store a secret as an opaque string
    client.secret_set(
        namespace="cephadm",
        scope=SecretScope.HOST,
        target="node1",
        name="ssh_key",
        data="AQB...==",
    )

    # Retrieve metadata (no data unless reveal=True)
    rec = client.secret_get("cephadm", SecretScope.HOST, "node1", "ssh_key")
    if rec:
        version = rec["metadata"]["version"]

    # Remove
    client.secret_rm("cephadm", SecretScope.HOST, "node1", "ssh_key")

CephSecretsClient methods
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Method
     - Description
   * - ``secret_set(namespace, scope, target, name, data, ...)``
     - Create or update a secret.  ``data`` must be a non-empty opaque
       string.  Increments the version and refreshes ``updated`` on each
       call; ``created`` is set only on the first write.  Optional
       ``user_made`` and ``editable`` flags control the stored policy;
       ``editable=False`` blocks future updates but does not prevent removal.
   * - ``secret_get(namespace, scope, target, name, reveal=False)``
     - Return the secret's metadata dict, plus the opaque ``data`` string if
       *reveal* is True.  Returns ``None`` if the secret does not exist.
   * - ``secret_get_value(namespace, scope, target, name)``
     - Return the stored opaque string directly, or ``None`` if the secret
       does not exist.  Use this when only the value is needed and the
       metadata envelope is not required.
   * - ``secret_get_version(namespace, scope, target, name)``
     - Return the current version integer, or ``None`` if not found.  A
       cheaper alternative to ``secret_get`` when only change-detection is needed.
   * - ``secret_get_versions(uris)``
     - Batch-fetch version numbers for a list of canonical ``secret:/...``
       URIs.  Returns a dict keyed by URI; missing secrets map to ``None``.
       Malformed URIs are skipped entirely, so a missing key indicates
       malformed input rather than an absent secret.
   * - ``secret_rm(namespace, scope, target, name)``
     - Remove a secret.  Idempotent: returns ``False`` if not found.
   * - ``secret_get_epoch(namespace)``
     - Return the current epoch for the namespace.  Use as a cheap
       change-detector before a full refresh.
   * - ``scan_refs(obj, namespace)``
     - Walk a JSON-like object and return secret-like reference strings found
       within it.  This includes valid whole-value secret URIs and malformed
       or embedded secret-like references that should be reported to the
       caller.  Only canonical ``secret:/...`` URIs should be passed to
       ``secret_get_versions``.
   * - ``scan_unresolved_refs(obj, namespace)``
     - Like ``scan_refs`` but returns only references that are missing,
       malformed, embedded inside a larger string, or cannot currently be
       read successfully.  Useful for pre-flight validation.
   * - ``resolve_object(obj)``
     - Walk a JSON-like object and replace every whole-value ``secret:/...``
       URI string with the stored opaque string for the referenced secret.

.. _mgr-ceph-secrets-scan:

Secret URI embedding and resolution
-----------------------------------

Configuration objects that need to reference secrets without embedding
plaintext can store ``secret:/...`` URIs as string values.  The module
resolves them on demand:

.. code-block:: python

    spec = {
        "service_type": "my-service",
        "spec": {
            "api_token": " secret:/cephadm/service/my-service/api_token ",
        },
    }

    # Check for missing, malformed, embedded, or unresolvable secret references
    unresolved = client.scan_unresolved_refs(spec, namespace="cephadm")
    if unresolved:
        raise RuntimeError(f"Unresolved secret references: {unresolved}")

    # Replace URI references with plaintext values
    resolved = client.resolve_object(spec)
    # resolved["spec"]["api_token"] now holds the stored API token

.. note::

   Resolution replaces the entire string value of a field.  Surrounding
   whitespace around a URI reference is ignored, so values such as
   ``" secret:/cephadm/global/foo "`` are accepted.  Embedding a secret URI
   inside a larger string (for example ``"Bearer secret:/..."``) is not
   supported; after trimming surrounding whitespace, the field value must be
   the URI.

   The resolved value is the stored opaque string.  The module does not parse
   stored data as JSON and does not support field-level URI selection.  If a
   caller stores structured data, it must encode and decode that structure
   itself.

.. _mgr-ceph-secrets-epoch:

Epoch-based change detection
----------------------------

Consumers that maintain a local cache of secrets can use the epoch to avoid
unnecessary full refreshes:

.. code-block:: python

    def maybe_refresh(client, namespace, last_epoch):
        current_epoch = client.secret_get_epoch(namespace)
        if current_epoch == last_epoch:
            return last_epoch   # nothing changed
        # ... do a full refresh ...
        return current_epoch

    last_epoch = 0
    last_epoch = maybe_refresh(client, "cephadm", last_epoch)

The epoch is incremented by every successful ``set`` operation and by
``rm`` only when an existing secret is actually removed (an idempotent
``rm`` returning ``"not_found"`` does not bump the epoch).  It starts at 0
for a namespace that has never been written to, and mutations in one namespace
never affect another namespace's epoch.


Configuration
=============

.. mgr_module:: ceph_secrets
.. confval:: secrets_backend

   :type: str
   :default: ``mon``

   The storage backend used for secrets.  Currently only the Mon KV store
   (``mon``) is supported.  This option is reserved for future backends
   (e.g. HashiCorp Vault).
