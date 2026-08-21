.. _auditdb-logging:

====================
Persistent Audit Logging with AuditDB
====================

Motivation
----------

Ceph already maintains an audit log for important operations performed on a
cluster, including administrative and recovery commands. However, the existing
audit log is a free-form text log and is not intended to serve as long-term
persistent audit history. Audit records may be rotated, deleted, or otherwise
lost, even though operations performed during a recovery may become relevant
months or years later when investigating a subsequent failure. CephFS 
disaster-recovery operations need to record important operations as well.

Persistent audit logging addresses these limitations by storing structured audit
records on RADOS in a .audit pool. Records can therefore be retained independently 
of the traditional text audit log and queried later to reconstruct important operations
performed on the cluster. The audit infrastructure provides a common storage
model for different producers while allowing producer-specific information to
be stored as structured JSON data.

Architecture Overview
---------------------

The audit logging stack has three components:

``AuditDB`` (``src/common/AuditDB.{h,cc}``)
  Generic persistence layer. Exposes interface for recording, querying,
  and maintaining audit records.

``ToolsAuditLogger`` (``src/tools/cephfs/ToolsAuditLogger.{h,cc}``)
  Standalone-tool wrapper around ``AuditDB``.  Simplifies the
  begin/intermediate/end recording pattern that every standalone DR tool 
  follows and handles table creation and event lifecycle automatically.

``auditman`` (``src/tools/cephfs/auditman.cc``)
Command-line interface for querying and inspecting audit records and channels
persisted through ``AuditDB``.

Underneath all three components, databases are stored as objects in the
``.audit`` RADOS pool via the ``cephsqlite`` VFS. 

AuditDB Storage Model
---------------------

The ``.audit`` Pool
^^^^^^^^^^^^^^^^^^^

All audit databases live in a dedicated RADOS pool named ``.audit``. ``AuditDB`` 
can ensure that this pool exists before an audit database is opened.

The Ceph SQLite VFS
^^^^^^^^^^^^^^^^^^^

``AuditDB`` opens its database through the ``cephsqlite`` VFS.
Database filenames follow the SQLite URI format::

    file:///.audit:/<name>_audit.db?vfs=ceph


One Database Per Channel
^^^^^^^^^^^^^^^^^^^^^^^^^

Each channel/producer/tool gets its own database file and its own
table within that database, named after the producer.  There is no
shared database between tools.

Schema
^^^^^^

Every audit table has the same four columns:

``seq`` (INTEGER PRIMARY KEY)
  Monotonically increasing record identifier.

``record_time`` (INTEGER)
  Unix timestamp when the record was committed.

``event_id`` (TEXT)
  UUID string that groups related records into a single logical event.

``json_dump`` (TEXT)
  JSON payload consisting of channel specific information. The schema 
  of ``json_dump`` depends on the writer to AuditDB.

Records and Events
------------------

A **record** is a single row in the audit table.  It is identified and
ordered by ``seq`` and carries one ``json_dump`` payload.

An **event** is a group of records that together describe one logical
operation.  All records belonging to the same event share the same
``event_id``.

When ``AuditDB::commit()`` is called without an ``event_id``, ``AuditDB``
generates and returns a new UUID. The caller stores that UUID and passes 
it back to subsequent ``commit()`` calls for the same event. 

Immutability
^^^^^^^^^^^^

Records are never updated in place.  The begin and end of an event are
separate rows with the same ``event_id``, not two fields in one row.
Intermediate records (if any) are additional rows under the same
``event_id``. Records within an event can be ordered by seq.

AuditDB Interface
-----------------

Initialization
^^^^^^^^^^^^^^

``init()``
Opens the database through the Ceph SQLite VFS, configures the database, 
creates the required schema, and prepares it for use.

Recording
^^^^^^^^^

``AuditDB`` has two ``commit()`` overloads gated by the mode the
instance was constructed in.

**Daemon mode** (``is_standalone = false``):

The caller supplies the ``seq``, which must be positive and unique
within the table.  Calling this overload on a standalone instance
returns ``seq_not_allowed_standalone``.

**Standalone mode** (``is_standalone = true``):

SQLite auto-assigns the next available integer as ``seq``.  Calling
this overload on a daemon instance returns ``seq_required``.

``commit()`` also has an optional event_id parameter. If passed in, 
the record being logged will be associated with an ongoing event. 
Otherwise, ``AuditDB`` will generate and assign a new event_id to the 
current record.

Both overloads return the ``event_id`` of the inserted row on success.
The caller should capture this value and pass it to subsequent
``commit()`` calls that belong to the same event.

Querying
^^^^^^^^

``query(const AuditQuery& q)``
  Returns a ``std::vector<AuditEntry>`` of matching rows.  Each
  ``AuditEntry`` carries ``seq``, ``record_time``, ``event_id``, and
  ``json_dump``.  Filtering, ordering, and limiting all occur inside
  SQLite; See `Query Model`_ for the full set of supported filters.

``count(const AuditQuery& q)``
  Returns the number of rows that match the filters in ``q``.

Query Model
-----------

Queries are expressed through the ``AuditQuery`` struct:

.. code-block:: c++

    struct AuditQuery {
      std::optional<int64_t> before_seq;
      std::optional<int64_t> after_seq;
      std::optional<time_t>  since;
      std::optional<time_t>  until;
      std::optional<std::string> event_id;
      std::vector<JsonFilter> json_filters;
      std::string order_by  = "record_time";
      bool        ascending = false;
      int64_t     limit     = 100;
    };

``before_seq`` / ``after_seq``
  Bound the result set by sequence number.

``since`` / ``until``
  Bound the result set by ``record_time``.

``event_id``
  Return only rows belonging to this event UUID.

``json_filters``
  A list of ``{ field, value }`` pairs.  Each pair is translated into a
  ``json_extract(json_dump, '$.<field>') = ?`` predicate in the SQL
  ``WHERE`` clause.  Multiple filters are combined with ``AND``.

``order_by`` / ``ascending``
  Sort column (``seq`` or ``record_time``) and direction.  Only these
  two columns are accepted to prevent SQL injection.

``limit``
  Maximum number of rows to return.

All filtering and limiting is performed at the database level inside
``AuditDB::query()``; no rows are fetched and then discarded by the
caller. The built SQL looks like::

    SELECT seq, record_time, event_id, json_dump
    FROM <table>
    [WHERE ...]
    ORDER BY <col> ASC|DESC
    LIMIT ?;

``auditman`` is the primary querying interface.  Its ``--format json``
output is designed to be piped to ``jq`` for complex filtering and
transformations that go beyond what ``AuditQuery`` supports natively.
See :ref:`auditman` for the full CLI reference and ``jq`` examples.

ToolsAuditLogger - Audit Logging Interface Specifically for CephFS DR Tools 
----------------

``ToolsAuditLogger`` wraps ``AuditDB`` with the recording pattern that
all DR tools follow.  It manages pool creation and the begin/intermediate/end 
event structure so that tool code only needs to call three methods.

Construction
^^^^^^^^^^^^

``ToolsAuditLogger`` cannot be constructed directly.  Use one of the
two static factory methods:

``create_for_tool(cct, rados, table_name)``
  Derives the database URI automatically as
  ``file:///.audit:/<table_name>_audit.db?vfs=ceph``, calls
  ``ensure_audit_pool()``, and opens the database.  This is the normal
  entry point for DR tools.

``create(cct, rados, db_uri, table_name)``
  Accepts an explicit URI.  Useful for testing or non-standard
  deployments.

Both methods ensure the ``.audit`` pool exists before attempting to
open the database.

Recording
^^^^^^^^^

``log_begin(cmd, cmd_args, init_time)``
  Commits the opening record for a tool invocation.  The JSON payload
  has the schema ``{ cmd, cmd_args, init_time, comp_time:null,
  status:null, retval:null }``.  ``AuditDB`` generates a UUID for this
  record; ``ToolsAuditLogger`` stores it internally as the
  ``event_id_in_flight``.  Calling ``log_begin`` while another begin is
  already in flight is a no-op with an error logged.

``log_intermediate(timestamp, intermediate_log_data)``
  Commits an intermediate record under the same ``event_id_in_flight``.
  This is intended to capture significant steps *during* tool execution.
  The caller is responsible for building the JSON payload using
  ``JSONFormatter`` and passing the resulting string. ``ToolsAuditLogger`` 
  passes the string containing the json_payload as is to ``AuditDB::commit``.  

``log_end(comp_time, status, retval)``
  Commits the closing record under ``event_id_in_flight``.  The JSON
  payload has the schema ``{ cmd, cmd_args, init_time, comp_time,
  status, retval }`` with all fields populated.  Resets the in-flight
  state.

Typical usage in a DR tool's ``main()``:

.. code-block:: c++

    auto logger_r = ToolsAuditLogger::create_for_tool(
        g_ceph_context, rados, "cephfs_data_scan");
    if (!logger_r) {
        // log warning and continue without audit
    }
    auto& logger = *logger_r;

    logger->log_begin(argv[0],
                      ToolsAuditLogger::get_audit_cmd_args(args),
                      std::time(nullptr));
    int rc = run_tool();
    logger->log_end(std::time(nullptr),
                    rc == 0 ? "completed" : "failed", rc);

auditman
--------

``auditman`` is the CLI tool for querying AuditDB. It provides basic
query and filtering operations to retreive data from channels within 
AuditDB. It returns plain, json, or json-pretty ouput to the caller.
Callers are intended to pipe auditman's output to ``jq`` for more advanced
filtering requirements. Its internal data flow is:

.. code-block:: text

    CLI options
        ↓
    AuditQuery (struct built by apply_cli_opts)
        ↓
    AuditDB::query() / AuditDB::count()
        ↓
    formatting (plain / json / json-pretty)

``apply_cli_opts()`` validates all inputs and translates them into an
``AuditQuery``.

For plain output, results are rendered with ``TextTable``.  For JSON
output, the ``json_dump`` field is returned as a string by AuditDB and 
is parsed into a structured JSON object by auditman which is embedded 
as the ``data`` field in the output.

The JSON output format is deliberately designed to be piped to ``jq``
for queries beyond what ``AuditQuery`` supports natively (substring
matches, derived fields, cross-record aggregation, custom formatting).

See :ref:`auditman` for the full CLI reference, option descriptions,
and ``jq`` examples.

See Also
--------

* :ref:`auditman` — CLI reference and ``jq`` query examples
