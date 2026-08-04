.. _auditman:

========
auditman
========

Purpose
-------

``auditman`` is a command line tool for querying audit logs produced by CephFS
disaster recovery tools. These tools include:

* ``cephfs-data-scan``
* ``cephfs-meta-injection``
* ``cephfs-table-tool``
* ``cephfs-journal-tool``

Audit logs are stored in a SQLite database in the ``.audit`` pool and provide a
complete record of tool invocations, including command, arguments, execution time,
status, and return value. This allows administrators to track which operations were
performed, when they ran, and whether they succeeded.

Typical use cases include:

* Troubleshooting failed tool invocations
* Tracking administrative operations on the filesystem
* Investigating the history of disaster recovery attempts

Audit Entry Fields
------------------

Each audit entry contains the following fields:

**Fields:**

* ``event_id`` — UUID identifying the event being logged. All events have a begin and end log entry and may have intermediary log entries. 
* ``seq`` — Sequence number assigned by the audit database.
* ``record_time`` — Timestamp when the entry was recorded (in local time).
* ``data`` - JSON dump of fields specific to DR tool.

**Data (JSON):**

* ``cmd`` — The tool command that was executed.
* ``cmd_args`` — Arguments passed to the tool.
* ``init_time`` — Timestamp when the tool started execution.
* ``comp_time`` — Timestamp when the tool completed (null for begin entry).
* ``status`` — Execution status (null for begin entry, or one of: ``ok``, ``failed``, ``aborted``).
* ``retval`` — Exit code (null for begin entry).

Notes
-----
* Each tool invocation generates two audit entries: a begin entry (recorded when
  the tool starts) and an end entry (recorded when the tool completes). There can 
  be intermediary entries, logging steps taken during execution of a tool. These 
  can be matched using the ``event_id`` field and ordered using the ``seq`` field.

* Audit data is stored in the ``.audit`` pool using a SQLite database managed
  by the libcephsqlite VFS. The database is accessible as a normal object in the
  pool.

* For advanced filtering and transformation of audit records, ``auditman`` is designed
  to output JSON that can be piped to `jq <https://jqlang.org/>`_ for
  sophisticated queries. See :ref:`Advanced Filtering with jq<advanced-jq-filtering>`
  for examples.

Syntax
------

.. parsed-literal::

    auditman [:ref:`general-options<auditman_general_options>`] --tool-name <tool> [:ref:`query-options<auditman_query_options>`]

General Options
^^^^^^^^^^^^^^^

.. _auditman_general_options:

.. option:: -h, --help

   Produce help message and exit.

.. option:: -c, --conf <path>

   Path to Ceph configuration file.

.. option:: --id <id>

   Client ID to use for authentication (default: ``admin``).

.. option:: -k, --keyring <path>

   Path to keyring file for authentication.

Query Options
^^^^^^^^^^^^^

.. _auditman_query_options:

.. option:: --tool-name <tool>

   **Required.** The audit channel to query. Must be one of:

   * ``cephfs-data-scan``
   * ``cephfs-meta-injection``
   * ``cephfs-table-tool``
   * ``cephfs-journal-tool``

Filtering
~~~~~~~~~

.. option:: --limit <n>

   Cap the number of rows returned (default: 100). Set to 0 for unlimited results.

.. option:: --before-seq <seq>

   Return only audit entries whose sequence number is less than ``seq``.

.. option:: --after-seq <seq>

   Return only audit entries whose sequence number is greater than ``seq``.

.. option:: --event-id <uuid>

   Return only entries belonging to the specified event ID (UUID). A single
   invocation of a tool generates two audit entries (begin and end) that share
   the same event_id.

.. option:: --filter <field=value>

   Filter results by a JSON field within the *data* field of returned records. The format is
   ``field=value`` and can be repeated for multiple filters.

   Example: ``--filter status=ok --filter cmd=data-scan``

   Common fields include: ``status`` (ok/failed/aborted), ``cmd`` (tool name),
   ``retval`` (exit code).

Time-based Filtering
~~~~~~~~~~~~~~~~~~~~

Specify one of the following time-based options. They are mutually exclusive:

.. option:: --since <timestamp>

   Return entries recorded on or after this timestamp. Format: ``YYYY-MM-DD``
   or ``YYYY-MM-DD HH:MM:SS``.

.. option:: --until <timestamp>

   Return entries recorded on or before this timestamp. Format: ``YYYY-MM-DD``
   or ``YYYY-MM-DD HH:MM:SS``.

.. option:: --range <start,end>

   Return entries within an inclusive timestamp range. Format:
   ``YYYY-MM-DD,YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS``.

   Example: ``--range "2024-01-01 10:00:00,2024-01-01 12:00:00"``

.. option:: --last <hours>

   Return entries from the last N hours before now. Accepts decimals
   (e.g., ``0.5`` for the last 30 minutes).

Result Options
~~~~~~~~~~~~~~

.. option:: --recent <n>

   Return the N most recent entries, ordered by timestamp descending.
   This is a convenience option for the most common query pattern.

.. option:: --order-by <column>

   Column to sort results by. Valid values: ``seq`` (default), ``record_time``.

.. option:: --order <direction>

   Sort direction: ``ASC`` or ``DESC`` (default: ``DESC``).

.. option:: --count

   Print the number of records that match provided filters, instead of listing them.

.. option:: --format <format>

   Output format. Valid values:

   * ``plain`` (default) — Human-readable table format
   * ``json`` — Compact JSON format
   * ``json-pretty`` — Pretty-printed JSON format

Examples
--------

Basic query — list recent invocations of a tool:

::

    auditman --tool-name cephfs-data-scan --recent 10

Query with time range:

::

    auditman --tool-name cephfs-journal-tool \
      --range "2024-01-15 08:00:00,2024-01-15 18:00:00"

Filter by status:

::

    auditman --tool-name cephfs-data-scan --filter status=failed

Query with event ID — view begin and end entries for a specific invocation:

::

    auditman --tool-name cephfs-meta-injection --event-id 12345678-1234-1234-1234-123456789abc

Get a count of successful operations:

::

    auditman --tool-name cephfs-data-scan --filter status=ok --count

JSON output for parsing:

::

    auditman --tool-name cephfs-table-tool --recent 5 --format json

.. _advanced-jq-filtering:

Advanced Filtering with jq
---------------------------

While ``auditman`` provides basic filtering via ``--filter``, ``--since/--until``, and
``--order-by``, the JSON output format is designed to be used with `jq <https://jqlang.org/>`_
for more sophisticated queries and transformations. The ``jq`` tool allows you to:

* Select specific fields
* Perform complex filtering on JSON data fields
* Calculate derived values (e.g., command duration)
* Re-format output for integration with other tools

Output Structure
~~~~~~~~~~~~~~~~

When using ``--format json`` or ``--format json-pretty``, auditman returns an array
of objects with the following structure::

 [
   {
     "event_id": "UUID",
     "seq": 1,
     "record_time": "2026-07-17 20:15:30",
     "data": {
       "cmd": "cephfs-journal-tool",
       "cmd_args": "--rank=a:0 event get summary",
       "init_time": 1721256930,
       "comp_time": 1721256935,
       "status": "completed",
       "retval": 0
     }
   }
 ]

Common jq Query Examples
~~~~~~~~~~~~~~~~~~

Select specific fields:

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq '[.[] | {seq, init_time, cmd: .data.cmd, command_args: .data.cmd_args}]'

Filter entries containing specific command arguments:

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq '[.[] | select(.data.cmd_args | contains("event get"))]'

Find all journal export operations:

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq '[.[] | select(.data.cmd_args | contains("journal export"))]'

Calculate command execution time in seconds:

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq '[.[] | {seq, command: .data.cmd_args, duration_seconds: (.data.comp_time - .data.init_time)}]'

Format output as table:

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq -r '.[] | "\(.seq)\t\(.record_time)\t\(.data.status)\t\(.data.cmd_args)"'

Sort by completion time (descending):

::

   auditman --tool-name cephfs-journal-tool --format json | \
     jq 'sort_by(.data.comp_time) | reverse'

Find failed operations with their error codes:

::

   auditman --tool-name cephfs-data-scan --format json | \
     jq '[.[] | select(.data.status == "failed") | {seq, retval: .data.retval, args: .data.cmd_args}]'


See Also
--------

* :ref:`cephfs-data-scan <cephfs-data-scan>` — CephFS data pool scanner
* :ref:`cephfs-journal-tool <cephfs-journal-tool>` — CephFS journal tool
* :ref:`cephfs-table-tool <cephfs-table-tool>` — CephFS table tool
* :ref:`cephfs-meta-injection <cephfs-meta-injection>` — CephFS metadata injector
* `jq Manual <https://jqlang.org/manual/>`_ — JSON query language documentation
