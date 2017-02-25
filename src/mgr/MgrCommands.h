// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/* no guard; may be included multiple times */

// see MonCommands.h

COMMAND("pg stat", "show placement group status.",
	"pg", "r", "cli,rest")
COMMAND("pg getmap", "get binary pg map to -o/stdout", "pg", "r", "cli,rest")

COMMAND("pg dump "							\
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|delta|pools|osds|pgs|pgs_brief,n=N,req=false", \
	"show human-readable versions of pg map (only 'all' valid with plain)", "pg", "r", "cli,rest")
COMMAND("pg dump_json "							\
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|pools|osds|pgs,n=N,req=false", \
	"show human-readable version of pg map in json only",\
	"pg", "r", "cli,rest")
COMMAND("pg dump_pools_json", "show pg pools info in json only",\
	"pg", "r", "cli,rest")

COMMAND("pg ls-by-pool "		\
        "name=poolstr,type=CephString " \
	"name=states,type=CephChoices,strings=active|clean|down|scrubbing|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with pool = [poolname]", "pg", "r", "cli,rest")
COMMAND("pg ls-by-primary " \
        "name=osd,type=CephOsdName " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|scrubbing|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with primary = [osd]", "pg", "r", "cli,rest")
COMMAND("pg ls-by-osd " \
        "name=osd,type=CephOsdName " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|scrubbing|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg on osd [osd]", "pg", "r", "cli,rest")
COMMAND("pg ls " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|scrubbing|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with specific pool, osd, state", "pg", "r", "cli,rest")
COMMAND("pg map name=pgid,type=CephPgid", "show mapping of pg to osds", \
	"pg", "r", "cli,rest")
COMMAND("pg dump_stuck " \
	"name=stuckops,type=CephChoices,strings=inactive|unclean|stale|undersized|degraded,n=N,req=false " \
	"name=threshold,type=CephInt,req=false",
	"show information about stuck pgs",\
	"pg", "r", "cli,rest")
COMMAND("pg debug " \
	"name=debugop,type=CephChoices,strings=unfound_objects_exist|degraded_pgs_exist", \
	"show debug info about pgs", "pg", "r", "cli,rest")

COMMAND("pg scrub name=pgid,type=CephPgid", "start scrub on <pgid>", \
	"pg", "rw", "cli,rest")
COMMAND("pg deep-scrub name=pgid,type=CephPgid", "start deep-scrub on <pgid>", \
	"pg", "rw", "cli,rest")
COMMAND("pg repair name=pgid,type=CephPgid", "start repair on <pgid>", \
	"pg", "rw", "cli,rest")
