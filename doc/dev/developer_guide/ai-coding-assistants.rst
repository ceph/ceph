.. This document is inspired by the Linux kernel's equivalent policy at
   Documentation/process/coding-assistants.rst (GPL-2.0). The content
   has been independently written for Ceph.

.. _ai_coding_assistants:

=======================
AI Coding Assistants
=======================

This document provides guidance for AI tools and developers using AI assistance
when contributing to Ceph.

AI tools helping with Ceph development should follow the standard Ceph
development process:

- `Contributing to Ceph: A Guide for Developers <index>`_
- `Submitting Patches to Ceph <../../../SubmittingPatches>`_
- `Basic Workflow <basic-workflow>`_

Coding Style
------------

All contributions must follow the `Ceph Coding Style
<https://github.com/ceph/ceph/blob/main/CodingStyle>`_ guide (``CodingStyle``
in the root of the repository).

Licensing and Legal Requirements
---------------------------------

All contributions must comply with Ceph's licensing requirements.

Unless stated otherwise, the Ceph source code is distributed under the terms of
the LGPL-2.1 or LGPL-3.0. For full details, see the file `COPYING`_ in the
top-level directory of the source-code tree.

.. _`COPYING`: https://github.com/ceph/ceph/blob/main/COPYING

Signed-off-by and Developer Certificate of Origin
--------------------------------------------------

AI agents **MUST NOT** add ``Signed-off-by`` tags. Only humans can legally
certify the Developer Certificate of Origin (DCO). The human submitter is
responsible for:

- Reviewing all AI-generated code
- Ensuring compliance with licensing requirements
- Adding their own ``Signed-off-by`` tag to certify the DCO
- Taking full responsibility for the contribution

By adding a ``Signed-off-by`` tag, the submitter also certifies that they
have disclosed all AI assistance used in producing the contribution, as
required by this document.

See `Sign your work <../../../SubmittingPatches>`_ in ``SubmittingPatches.rst``
for the full DCO text and instructions.

Attribution
-----------

When AI tools contribute to Ceph development, proper attribution helps track
the evolving role of AI in the development process. Contributions should
include an ``Assisted-by`` tag in the following format (items in square
brackets are optional)::

    Assisted-by: AGENT_NAME-AGENT_VERSION[:MODEL_NAME] [TOOL1] [TOOL2]

Where:

- ``AGENT_NAME`` is the name of the AI tool or framework
- ``AGENT_VERSION`` is the version of the agent (required), appended with ``-``
- ``MODEL_NAME`` is the specific underlying model used, if applicable
- ``[TOOL1] [TOOL2]`` are optional specialised analysis tools used
  (e.g., ``clang-tidy``, ``clang-format``, ``cppcheck``, ``mypy``)

Basic development tools (git, gcc, make, editors) should not be listed.

Examples::

    Assisted-by: Claude-v2.1.199:claude-4-5-sonnet clang-tidy
    Assisted-by: IBM-Bob-v2.0.1 clangd

