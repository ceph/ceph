=============================
Ceph Release Manager
=============================

Purpose
=======
The Ceph Release Manager (CRM) coordinates the release lifecycle to ensure the
predictability, stability, and quality of Ceph releases. The role serves as a
centralized point of contact for release engineering, maintaining the
consistency of stable branches, and overseeing the execution of quality
assurance gates.

Function
========
The core function of the CRM is to guide releases from the stabilization phase
through to general availability. This involves:

* **Release Gatekeeping:** Determining the readiness of release candidates based
  on test results, upgrade coverage, and community feedback.
* **Branch Management:** Enforcing strict merge discipline on stable branches to
  prioritize release stability.
* **Dependency Coordination:** Overseeing changes to build dependencies,
  compiler features, and build tools on stable branches to prevent disruption
  during stabilization periods.

Responsibilities
================

Schedule and Cadence
--------------------
* Maintain and communicate a consistent release cadence for Major (X.2.z) and
  Minor (x.2.Z) releases.
* Define and manage merge windows to ensure code has adequate stabilization
  time.

Branch Discipline and Gatekeeping
---------------------------------
* **Main Branch Autonomy:** The CRM does not actively police or gate merges
  into the ``main`` development branch.
* **Single Release Branch Policy:** Maintain a single ``$release`` branch for
  each stable version. Alternate branches will be made as necessary to issue
  hotfixes and release candidates. The legacy staging branch
  ``$release-release`` is no longer used to keep the release branch open for
  merges. This is to enforce release discipline, maintain a stable release
  branch, allow for regular upgrade testing from the release branch, and avoid
  confusing the provenance of backports caused by cherry-picking to the release
  staging branch.
* **GitHub Branch Protections:** The CRM manages branch protection rules to
  safeguard stability. End of Life (EOL) branches are marked read-only.
  Living release branches utilize protection rules mirroring the ``main``
  development branch. Furthermore, a specific "Release Branch Protection"
  GitHub ruleset restricts direct modifications and merge privileges
  exclusively to the Ceph Release Manager team. This prevents accidental
  or unauthorized merges by component leads without CRM approval.
* **Upstream-First Policy:** Enforce that bug fixes must be merged into the
  ``main`` branch before being backported to a ``$release`` branch, barring
  exceptional release-specific build or integration issues.

Quality Assurance and Automation
--------------------------------
* **Upgrade Testing:** Ensure comprehensive upgrade test coverage exists and is
  successfully executed to validate safe upgrade paths for end-users. Another
  reason to freeze the branch is to have upgrade tests (for other branches)
  QA'ing the actual code being planned to ship, not acting as a testing ground.
* **Backport Automation:** Ensure that functional and reliable automations exist
  for backporting changes to stable release branches.
* **Release Tooling:** Maintain and improve the CI/CD automation related to the
  release pipeline to reduce manual toil.

Artifact Generation and Signing
-------------------------------
* **Build Pipeline:** The CRM initiates the generation of release artifacts
  via the `ceph-release-pipeline
  <https://jenkins.ceph.com/view/all/job/ceph-release-pipeline/build>`_
  Jenkins job.
* **Cryptographic Signing:** The CRM is responsible for establishing and
  maintaining the process for the cryptographic signing of releases, whether
  utilizing historical Ceph GPG keys or implementing modern signing methods.

Security and Lifecycle Management
---------------------------------
* **CVE Coordination:** Act as the central coordinator for embargoed security
  releases, ensuring that CVE patches are cleanly backported, tested, and
  staged for coordinated public disclosure.
* **End of Life (EOL) Management:** Manage the lifecycle of stable branches.
  Ceph officially supports the two most recent major releases (N-2). Shortly
  after a new major release is published, the CRM transitions the N-3 release
  to EOL status by closing all open backport PRs, closing associated backport
  tracker tickets, and freezing the branch.

Communication and Visibility
----------------------------
* **Release Engineering Meeting:** Host an at-least weekly "Ceph Release
  Engineering" call to facilitate upstream and community discussion regarding
  upcoming releases and the release process.
* **Release Notes:** Ensure effective, accurate, and comprehensive documentation
  for releases, including modernized release notes and public mailing list
  announcements.
* **Tracker Management:** Maintain the issue tracker to clearly surface release
  status and highlight "Blocker" issues.
* **Blocker Definition:** A "Blocker" is strictly defined as a bug resulting
  in data loss, data corruption, or an issue of similarly critical severity
  that prevents safe deployment.

Major Release Process
=====================
While minor releases follow a strict cadence, major releases follow a distinct,
annual lifecycle. The ``main`` branch accumulates changes throughout the year,
with a general target of a Spring release. 

* **Distribution Matrix:** A critical gatekeeping duty of the CRM for major
  releases is establishing and maintaining the matrix of supported
  distributions (e.g., supported OS versions and architectures) for the
  lifespan of that release.
* **Release Kickoff:** Shortly after a major release is branched off of
  ``main``, the CRM or a designated deputy executes the official release
  kickoff process. See the `Major Release Kickoff Checklist
  <http://LINK_TO_KICKOFF_DOC>`_ for detailed steps.
* **Release Candidates:** The formalization of Release Candidates (RCs) for
  major releases is currently deferred pending an upcoming overhaul of the
  Ceph versioning schema.

Minor Release Blueprint
=======================
The CRM orchestrates minor releases using a structured 8-week cycle. To ensure
predictable delivery, the two actively supported ("live") releases are scheduled
with non-overlapping merge windows. These merge windows may be offset or shifted
as necessary in the event of minor release delays to maintain the staggered
cadence.

Release Tracker
--------------
The release cycle is initiated by creating an authoritative tracker ticket in the
`Stable Releases tracker <https://tracker.ceph.com/projects/ceph-releases>`_
to manage and document the work of the release.

Phase 1: Merge Window (4 Weeks)
-------------------------------
* **Tagging:** Component leads or the CRM label open backport Pull Requests
  (PRs) with the ``needs-qa`` label.
* **Aggregation & QA:** The CRM (or a designated deputy) aggregates the tagged
  PRs into testing suites for Quality Assurance execution.
* **Review & Gatekeeping:** The CRM coordinates the review of QA results to
  determine suitability for merging. The CRM actively gates PRs during this
  phase to ensure branch stability is maintained before code is merged.

Phase 2: QA and Stabilization (4 Weeks)
---------------------------------------
* **Validation:** Dedicated time allocated for extensive QA testing, regression
  checks, and the validation required to push out the minor release.
* **Silent Period:** Any unused time remaining in this 4-week allocation acts
  as a "silent" stabilization period before the release is officially cut and
  published.

Out-of-Band (Hotfix) Releases
=============================
Critical vulnerabilities (e.g., zero-day CVEs) or severe regressions (e.g.,
data loss bugs) may necessitate an out-of-band "hotfix" release. These
releases bypass the standard 8-week minor release blueprint and are managed
on an expedited timeline. All out-of-band releases must be strictly
coordinated with the CRM.
