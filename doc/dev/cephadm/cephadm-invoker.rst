Cephadm Invoker
===============

The ``cephadm_invoker.py`` script provides a wrapper intended for executing
cephadm commands with limited sudo priviliges. It is used when sudo hardening
is enabled.

Overview
--------

The cephadm invoker validates the cephadm binary hash before execution and
provides a secure way to run cephadm commands and deploy binaries. It is installed as
part of the cephadm RPM at ``/usr/libexec/cephadm_invoker.py``.

Commands
--------

The invoker supports the following subcommands:

``run``

Execute cephadm binary with arguments after hash verification::

    cephadm_invoker.py run <binary> [args...]

The binary path must include a hash in the filename for verification.

``deploy_binary``

Deploy cephadm binary from a temporary file to the final location::

    cephadm_invoker.py deploy_binary <temp_file> <final_path>

``check_binary``

Check if a cephadm binary exists::

    cephadm_invoker.py check_binary <cephadm_binary_path>

Returns exit code 0 if the file exists, 2 if it does not exist.

Exit Codes
----------

- ``0``: Success
- ``1``: General error (file not found, permission issues, etc.)
- ``2``: Binary hash mismatch or file doesn't exist (triggers redeployment)
- ``126``: Permission denied during execution

Security Features
-----------------

The cephadm invoker provides the following security features:

- **Binary Hash Verification**: Validates the cephadm binary integrity before execution
- **Restricted Execution**: Only allows execution of verified cephadm binaries
- **Secure Deployment**: Safely deploys cephadm binaries with proper permissions
- **Logging**: Comprehensive logging to both console and syslog for audit trails
