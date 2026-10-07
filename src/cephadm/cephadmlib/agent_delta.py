# agent_delta.py - delta metadata tracking for the cephadm agent
#
# The agent reports four metadata sections to the mgr: ls, networks, facts
# and volume. When the mgr enables delta payloads, a section whose content
# has not changed since it was last delivered is omitted from the report and
# listed under 'unchanged' instead.
#
# Rules this module implements:
#
# * Every section is sent in full the first time it is reported for a given
#   ack (full sync). A new ack is the mgr's request for fresh metadata.
# * A section's baseline only moves after the mgr confirms that it processed
#   the report (see MetadataDeltaTracker.commit). A lost or rejected report
#   leaves every section eligible for retransmission.
# * The mgr can ask for a section to be resent in full (resync), e.g. after a
#   mgr restart when it no longer holds that section.
# * ls is compared per daemon. memory_usage and cpu_percentage are compared
#   against the last delivered value with a tolerance; every other field is
#   compared exactly.

import enum
import hashlib
import json

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
)

METADATA_SECTIONS = ('ls', 'networks', 'facts', 'volume')


class DeltaReason(str, enum.Enum):
    """Why a section was, or was not, included in a report.

    MEMORY and CPU only apply to ls. The ls reasons of delivered reports are
    counted and sent to the mgr as 'ls_delta_stats', keyed by value; the mgr's
    DeltaReason in src/pybind/mgr/cephadm/agent_metrics.py must use the same
    values. Use .value when formatting a member as text.
    """

    FULL_SYNC = 'full_sync'
    UNCHANGED = 'unchanged'
    STRUCTURAL = 'structural'
    MEMORY = 'memory_usage'
    CPU = 'cpu_percentage'


# Host facts that are observations rather than host properties. They change
# on every collection and must not make facts look changed.
VOLATILE_FACTS = (
    'timestamp',
    'system_uptime',
    'memory_free_kb',
    'memory_available_kb',
    'cpu_load',
)

# ls fields compared with a tolerance; every other ls field is compared
# exactly.
LS_STATS_FIELDS = ('memory_usage', 'cpu_percentage')
MEMORY_TOLERANCE_BYTES = 10 * 1024 * 1024
CPU_TOLERANCE_PERCENT = 1.0


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def canonicalize(value: Any) -> Any:
    """Return value with dict keys and list items in a deterministic order."""
    if isinstance(value, dict):
        return {
            key: canonicalize(item) for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        items = [canonicalize(item) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(',', ':')
            ),
        )
    return value


def normalized_facts(facts_json: str) -> Any:
    facts = json.loads(facts_json)
    for key in VOLATILE_FACTS:
        facts.pop(key, None)
    if isinstance(facts.get('sysctl_options'), dict):
        # The mgr only uses sysctl option names (tuned profile validation).
        # Values such as kernel.random.uuid change on every read.
        facts['sysctl_options'] = sorted(facts['sysctl_options'])
    return canonicalize(facts)


def ls_by_name(ls: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index ls entries by daemon name so listing order does not matter."""
    by_name: Dict[str, Dict[str, Any]] = {}
    for entry in ls:
        # An entry without a name is keyed by its content, so any change to
        # it shows up as a removed and an added daemon.
        name = str(entry.get('name', '')) or stable_hash(entry)
        by_name[name] = entry
    return by_name


def section_baseline(name: str, value: Any) -> Any:
    """Return what a section is compared against on the next report.

    For ls this is the delivered entries themselves, because memory and CPU
    are compared with a tolerance. For every other section it is a hash of
    the section's canonical form.
    """
    if name == 'ls':
        return ls_by_name(value)
    if name == 'facts':
        return stable_hash(normalized_facts(value))
    if name == 'volume':
        try:
            return stable_hash(canonicalize(json.loads(value)))
        except (TypeError, ValueError):
            return stable_hash(value)
    return stable_hash(value)


def _parse_bytes(value: Any) -> float:
    return float(value)


def _parse_percent(value: Any) -> float:
    return float(str(value).strip('%'))


def stat_changed(
    previous: Any,
    current: Any,
    tolerance: float,
    parse: Callable[[Any], float],
) -> bool:
    if current == previous:
        # Also covers None == None: stats can be missing on both reports.
        return False
    if current is None or previous is None:
        return True
    try:
        return abs(parse(current) - parse(previous)) > tolerance
    except (TypeError, ValueError):
        return True


def ls_change_reason(
    previous: Dict[str, Dict[str, Any]],
    current: Dict[str, Dict[str, Any]],
    memory_tolerance: float = MEMORY_TOLERANCE_BYTES,
    cpu_tolerance: float = CPU_TOLERANCE_PERCENT,
) -> Optional[DeltaReason]:
    """Classify how ls changed, or return None if it did not.

    The precedence is structural, then memory, then CPU, regardless of daemon
    or field order. A report is attributed to a stat only when nothing else
    changed in any daemon.
    """
    if set(previous) != set(current):
        return DeltaReason.STRUCTURAL
    for name, entry in current.items():
        old = previous[name]
        if set(old) != set(entry):
            return DeltaReason.STRUCTURAL
        for key, value in entry.items():
            if key in LS_STATS_FIELDS:
                continue
            if stable_hash(value) != stable_hash(old[key]):
                return DeltaReason.STRUCTURAL
    if any(
        stat_changed(
            previous[name].get('memory_usage'),
            entry.get('memory_usage'),
            memory_tolerance,
            _parse_bytes,
        )
        for name, entry in current.items()
    ):
        return DeltaReason.MEMORY
    if any(
        stat_changed(
            previous[name].get('cpu_percentage'),
            entry.get('cpu_percentage'),
            cpu_tolerance,
            _parse_percent,
        )
        for name, entry in current.items()
    ):
        return DeltaReason.CPU
    return None


class PendingSection(NamedTuple):
    """A section evaluated for one report, awaiting the mgr's response."""

    ack: int
    baseline: Any
    reason: DeltaReason

    @property
    def sent(self) -> bool:
        return self.reason != DeltaReason.UNCHANGED


class MetadataDeltaTracker:
    """Decides which metadata sections a report includes.

    Usage per report: build() the delta fields, send them, then commit() the
    returned pending sections only if the mgr processed the report.
    """

    def __init__(self) -> None:
        # Ack under which each section was last delivered in full.
        self.synced_ack: Dict[str, int] = {}
        # Last delivered value per section, see section_baseline().
        self.baselines: Dict[str, Any] = {}
        # Cumulative count of ls reasons for delivered reports.
        self.ls_reason_counts: Dict[DeltaReason, int] = {
            reason: 0 for reason in DeltaReason
        }

    def change_reason(
        self, name: str, baseline: Any, ack: int
    ) -> Optional[DeltaReason]:
        if self.synced_ack.get(name) != ack:
            return DeltaReason.FULL_SYNC
        previous = self.baselines.get(name)
        if previous is None:
            # synced_ack and baselines move together; recover from any
            # inconsistency by sending the section.
            return DeltaReason.STRUCTURAL
        if name == 'ls':
            return ls_change_reason(previous, baseline)
        return None if previous == baseline else DeltaReason.STRUCTURAL

    def build(
        self, ack: int, sections: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, PendingSection]]:
        """Return the report fields for sections, and the pending state.

        sections holds the metadata available for this report; a section
        that has not been gathered for the current ack must be left out.
        """
        fields: Dict[str, Any] = {}
        pending: Dict[str, PendingSection] = {}
        unchanged: List[str] = []
        for name, value in sections.items():
            if value is None:
                continue
            baseline = section_baseline(name, value)
            reason = self.change_reason(name, baseline, ack)
            if reason is None:
                reason = DeltaReason.UNCHANGED
                unchanged.append(name)
            else:
                fields[name] = value
            pending[name] = PendingSection(ack, baseline, reason)
        if unchanged:
            fields['unchanged'] = unchanged
        # Cumulative counters: a lost report loses no information, and they
        # only move in commit(), so retries are not counted.
        fields['ls_delta_stats'] = {
            reason.value: count
            for reason, count in self.ls_reason_counts.items()
        }
        return fields, pending

    def commit(self, pending: Dict[str, PendingSection]) -> None:
        """Record that the mgr processed the report built with pending."""
        for name, section in pending.items():
            if section.sent:
                # Unchanged sections keep their old baseline, so slow drift
                # in memory or CPU accumulates until it exceeds the tolerance.
                self.synced_ack[name] = section.ack
                self.baselines[name] = section.baseline
            if name == 'ls':
                self.ls_reason_counts[section.reason] += 1

    def resync(self, sections: Iterable[str]) -> None:
        """Send the given sections in full on the next report."""
        for name in sections:
            if name in METADATA_SECTIONS:
                self.synced_ack.pop(name, None)
                self.baselines.pop(name, None)
