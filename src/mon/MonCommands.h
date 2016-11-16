// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc. 
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/* no guard; may be included multiple times */

/*
 * Define commands that are reported by the monitor's
 * "get_command_descriptions" command, and parsed by the Python
 * frontend 'ceph' (and perhaps by other frontends, such as a RESTful
 * server). The format is:
 *
 * COMMAND(signature, helpstring, modulename, req perms, availability)
 * where:
 * signature:  describes the command and its parameters (more below)
 * helpstring: displays in CLI help, API help (nice if it refers to
 *             parameter names from signature, 40-a few hundred chars)
 * modulename: the monitor module or daemon this applies to:
 *             mds, osd, pg (osd), mon, auth, log, config-key
 * req perms:  required permission in that modulename space to execute command
 *             this also controls what type of REST command is accepted
 * availability: cli, rest, or both
 *
 * The commands describe themselves completely enough for the separate
 * frontend(s) to be able to accept user input and validate it against
 * the command descriptions, and generate a JSON object that contains
 * key:value mappings of parameter names to validated parameter values.
 *
 * 'signature' is a space-separated list of individual command descriptors;
 * each descriptor is either a literal string, which can contain no spaces or
 * '=' signs (for instance, in "pg stat", both "pg" and "stat" are literal
 * strings representing one descriptor each), or a list of key=val[,key=val...]
 * which also includes no spaces.  
 *
 * The key=val form describes a non-literal parameter.  Each will have at
 * least a name= and type=, and each type can have its own type-specific
 * parameters.  The parser is the arbiter of these types and their 
 * interpretation.  A few more non-type-specific key=val pairs exist:
 *
 *    req=false marks an optional parameter (default for req is 'true')
 *    n=<n> is a repeat count for how many of this argument must be supplied.
 *          n=1 is the default.
 *          n=N is a special case that means "1 or more".
 *
 * A perhaps-incomplete list of types:
 *
 * CephInt: Optional: range=min[|max]
 * CephFloat: Optional range
 * CephString: optional badchars
 * CephSocketpath: validation involves "is it S_ISSOCK"
 * CephIPAddr: v4 or v6 addr with optional port, syntax validated
 * CephEntityAddr: CephIPAddr + optional '/nonce'
 * CephPoolname: Plainold string
 * CephObjectname: Another plainold string
 * CephPgid: n.xxx where n is an int > 0, xxx is a hex number > 0
 * CephName: daemon name, '*' or '<type>.<id>' (id must be int for type osd)
 * CephOsdName: osd name, '*' or '<id> or 'osd.<id>' (id must be int)
 * CephChoices: strings="foo|bar" means this param can be either
 * CephFilepath: openable file
 * CephFragment: cephfs 'fragID': val/bits, val in hex 0xnnn, bits in dec
 * CephUUID: uuid in text matching Python uuid.UUID()
 * CephPrefix: special type assigned to literals
 *
 * Example:
 *
 * COMMAND("auth add " \
 *   	   "name=entity,type=CephString " \
 *   	   "name=caps,type=CephString,n=N,req=false", \
 *   	   "add auth info for <name> from input file, or random key " \
 *   	   "if no input given, and/or any caps specified in the command")
 *
 * defines a command "auth add" that takes a required argument "entity"
 * of type "CephString", and from 1 to N arguments named "caps" of type
 * CephString, at least one of which is required.  The front end will
 * validate user input against this description.  Let's say the user
 * enters auth add client.admin 'mon rwx' 'osd *'.  The result will be a
 * JSON object like {"prefix":"auth add", "entity":"client.admin",
 * "caps":["mon rwx", "osd *"]}.
 * Note that 
 * 	- string literals are accumulated into 'prefix'
 * 	- n=1 descriptors are given normal string or int object values
 * 	- n=N descriptors are given array values
 *
 * NOTE: be careful with spaces.  Each descriptor must be separated by
 * one space, no other characters, so if you split lines as above, be
 * sure to close and reopen the quotes, and be careful to include the '
 * separating spaces in the quoted string.
 *
 * The monitor marshals this JSON into a std::map<string, cmd_vartype>
 * where cmd_vartype is a boost::variant type-enforcing discriminated
 * type, so the monitor is expected to know the type of each argument.
 * See cmdparse.cc/h for more details.
 *
 * The flag parameter for COMMAND_WITH_FLAGS macro must be passed using
 * FLAG(f), where 'f' may be one of the following:
 *
 *  NONE      - no flag assigned
 *  NOFORWARD - command may not be forwarded
 *  OBSOLETE  - command is considered obsolete
 *  DEPRECATED - command is considered deprecated
 *
 * A command should always be first considered DEPRECATED before being
 * considered OBSOLETE, giving due consideration to users and conforming
 * to any guidelines regarding deprecating commands.
 */

/*
 * pg commands PgMonitor.cc
 */

COMMAND("pg stat", "show placement group status.", "pg", "r", "cli,rest")
COMMAND("pg getmap", "get binary pg map to -o/stdout", "pg", "r", "cli,rest")
COMMAND("pg send_pg_creates", "trigger pg creates to be issued",\
	"pg", "rw", "cli,rest")
COMMAND("pg dump " \
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|delta|pools|osds|pgs|pgs_brief,n=N,req=false", \
	"show human-readable versions of pg map (only 'all' valid with plain)", "pg", "r", "cli,rest")
COMMAND("pg dump_json " \
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|pools|osds|pgs,n=N,req=false", \
	"show human-readable version of pg map in json only",\
	"pg", "r", "cli,rest")
COMMAND("pg dump_pools_json", "show pg pools info in json only",\
	"pg", "r", "cli,rest")
COMMAND("pg dump_stuck " \
	"name=stuckops,type=CephChoices,strings=inactive|unclean|stale|undersized|degraded,n=N,req=false " \
	"name=threshold,type=CephInt,req=false",
	"show information about stuck pgs",\
	"pg", "r", "cli,rest")
COMMAND("pg ls-by-pool " \
        "name=poolstr,type=CephString " \
	"name=states,type=CephChoices,strings=active|clean|down|replay|splitting|scrubbing|scrubq|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with pool = [poolname | poolid]", "pg", "r", "cli,rest")
COMMAND("pg ls-by-primary " \
        "name=osd,type=CephOsdName " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|replay|splitting|scrubbing|scrubq|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with primary = [osd]", "pg", "r", "cli,rest")
COMMAND("pg ls-by-osd " \
        "name=osd,type=CephOsdName " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|replay|splitting|scrubbing|scrubq|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg on osd [osd]", "pg", "r", "cli,rest")
COMMAND("pg ls " \
        "name=pool,type=CephInt,req=false " \
	"name=states,type=CephChoices,strings=active|clean|down|replay|splitting|scrubbing|scrubq|degraded|inconsistent|peering|repair|recovering|backfill_wait|incomplete|stale|remapped|deep_scrub|backfill|backfill_toofull|recovery_wait|undersized|activating|peered,n=N,req=false ", \
	"list pg with specific pool, osd, state", "pg", "r", "cli,rest")
COMMAND("pg map name=pgid,type=CephPgid", "show mapping of pg to osds", \
	"pg", "r", "cli,rest")
COMMAND("pg scrub name=pgid,type=CephPgid", "start scrub on <pgid>", \
	"pg", "rw", "cli,rest")
COMMAND("pg deep-scrub name=pgid,type=CephPgid", "start deep-scrub on <pgid>", \
	"pg", "rw", "cli,rest")
COMMAND("pg repair name=pgid,type=CephPgid", "start repair on <pgid>", \
	"pg", "rw", "cli,rest")
COMMAND("pg debug " \
	"name=debugop,type=CephChoices,strings=unfound_objects_exist|degraded_pgs_exist", \
	"show debug info about pgs", "pg", "r", "cli,rest")
COMMAND("pg force_create_pg name=pgid,type=CephPgid", \
	"force creation of pg <pgid>", "pg", "rw", "cli,rest")
COMMAND("pg set_full_ratio name=ratio,type=CephFloat,range=0.0|1.0", \
	"set ratio at which pgs are considered full", "pg", "rw", "cli,rest")
COMMAND("pg set_nearfull_ratio name=ratio,type=CephFloat,range=0.0|1.0", \
	"set ratio at which pgs are considered nearly full", \
	"pg", "rw", "cli,rest")

/*
 * auth commands AuthMonitor.cc
 */

COMMAND("auth export name=entity,type=CephString,req=false", \
       	"write keyring for requested entity, or master keyring if none given", \
	"auth", "rx", "cli,rest")
COMMAND("auth get name=entity,type=CephString", \
	"write keyring file with requested key", "auth", "rx", "cli,rest")
COMMAND("auth get-key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth print-key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth print_key name=entity,type=CephString", "display requested key", \
	"auth", "rx", "cli,rest")
COMMAND("auth list", "list authentication state", "auth", "rx", "cli,rest")
COMMAND("auth import", "auth import: read keyring file from -i <file>", \
	"auth", "rwx", "cli,rest")
COMMAND("auth add " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"add auth info for <entity> from input file, or random key if no " \
        "input is given, and/or any caps specified in the command",
	"auth", "rwx", "cli,rest")
COMMAND("auth get-or-create-key " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"get, or add, key for <name> from system/caps pairs specified in the command.  If key already exists, any given caps must match the existing caps for that key.", \
	"auth", "rwx", "cli,rest")
COMMAND("auth get-or-create " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"add auth info for <entity> from input file, or random key if no input given, and/or any caps specified in the command", \
	"auth", "rwx", "cli,rest")
COMMAND("auth caps " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N", \
	"update caps for <name> from caps specified in the command", \
	"auth", "rwx", "cli,rest")
COMMAND("auth del " \
	"name=entity,type=CephString", \
	"delete all caps for <name>", \
	"auth", "rwx", "cli,rest")
COMMAND("auth rm " \
	"name=entity,type=CephString", \
	"remove all caps for <name>", \
	"auth", "rwx", "cli,rest")

/*
 * Monitor commands (Monitor.cc)
 */
COMMAND_WITH_FLAG("compact", "cause compaction of monitor's leveldb storage (DEPRECATED)", \
	     "mon", "rw", "cli,rest", \
             FLAG(NOFORWARD)|FLAG(DEPRECATED))
COMMAND_WITH_FLAG("scrub", "scrub the monitor stores (DEPRECATED)", \
             "mon", "rw", "cli,rest", \
             FLAG(DEPRECATED))
COMMAND("fsid", "show cluster FSID/UUID", "mon", "r", "cli,rest")
COMMAND("log name=logtext,type=CephString,n=N", \
	"log supplied text to the monitor log", "mon", "rw", "cli,rest")
COMMAND_WITH_FLAG("injectargs " \
	     "name=injected_args,type=CephString,n=N",			\
	     "inject config arguments into monitor", "mon", "rw", "cli,rest",
	     FLAG(NOFORWARD))
COMMAND("status", "show cluster status", "mon", "r", "cli,rest")
COMMAND("health name=detail,type=CephChoices,strings=detail,req=false", \
	"show cluster health", "mon", "r", "cli,rest")
COMMAND("df name=detail,type=CephChoices,strings=detail,req=false", \
	"show cluster free space stats", "mon", "r", "cli,rest")
COMMAND("report name=tags,type=CephString,n=N,req=false", \
	"report full status of cluster, optional title tag strings", \
	"mon", "r", "cli,rest")
COMMAND("quorum_status", "report status of monitor quorum", \
	"mon", "r", "cli,rest")

COMMAND_WITH_FLAG("mon_status", "report status of monitors", "mon", "r", "cli,rest",
	     FLAG(NOFORWARD))
COMMAND_WITH_FLAG("sync force " \
	"name=validate1,type=CephChoices,strings=--yes-i-really-mean-it,req=false " \
	"name=validate2,type=CephChoices,strings=--i-know-what-i-am-doing,req=false", \
	"force sync of and clear monitor store (DEPRECATED)", \
        "mon", "rw", "cli,rest", \
        FLAG(NOFORWARD)|FLAG(DEPRECATED))
COMMAND_WITH_FLAG("heap " \
	     "name=heapcmd,type=CephChoices,strings=dump|start_profiler|stop_profiler|release|stats", \
	     "show heap usage info (available only if compiled with tcmalloc)", \
	     "mon", "rw", "cli,rest", FLAG(NOFORWARD))
COMMAND("quorum name=quorumcmd,type=CephChoices,strings=enter|exit,n=1", \
	"enter or exit quorum", "mon", "rw", "cli,rest")
COMMAND("tell " \
	"name=target,type=CephName " \
	"name=args,type=CephString,n=N", \
	"send a command to a specific daemon", "mon", "rw", "cli,rest")
COMMAND_WITH_FLAG("version", "show mon daemon version", "mon", "r", "cli,rest",
                  FLAG(NOFORWARD))

COMMAND("node ls " \
	"name=type,type=CephChoices,strings=all|osd|mon|mds,req=false",
	"list all nodes in cluster [type]", "mon", "r", "cli,rest")
/*
 * Monitor-specific commands under module 'mon'
 */
COMMAND_WITH_FLAG("mon compact", \
    "cause compaction of monitor's leveldb storage", \
    "mon", "rw", "cli,rest", \
    FLAG(NOFORWARD))
COMMAND_WITH_FLAG("mon scrub",
    "scrub the monitor stores", \
    "mon", "rw", "cli,rest", \
    FLAG(NONE))
COMMAND_WITH_FLAG("mon sync force " \
    "name=validate1,type=CephChoices,strings=--yes-i-really-mean-it,req=false " \
    "name=validate2,type=CephChoices,strings=--i-know-what-i-am-doing,req=false", \
    "force sync of and clear monitor store", \
    "mon", "rw", "cli,rest", \
    FLAG(NOFORWARD))
COMMAND("mon metadata name=id,type=CephString",
	"fetch metadata for mon <id>",
	"mon", "r", "cli,rest")


/*
 * MDS commands (MDSMonitor.cc)
 */

COMMAND("mds stat", "show MDS status", "mds", "r", "cli,rest")
COMMAND("mds dump " 
	"name=epoch,type=CephInt,req=false,range=0", \
	"dump legacy MDS cluster info, optionally from epoch",
        "mds", "r", "cli,rest")
COMMAND("fs dump "
	"name=epoch,type=CephInt,req=false,range=0", \
	"dump all CephFS status, optionally from epoch", "mds", "r", "cli,rest")
COMMAND("mds getmap " \
	"name=epoch,type=CephInt,req=false,range=0", \
	"get MDS map, optionally from epoch", "mds", "r", "cli,rest")
COMMAND("mds metadata name=who,type=CephString",
	"fetch metadata for mds <who>",
	"mds", "r", "cli,rest")
COMMAND("mds tell " \
	"name=who,type=CephString " \
	"name=args,type=CephString,n=N", \
	"send command to particular mds", "mds", "rw", "cli,rest")
COMMAND("mds compat show", "show mds compatibility settings", \
	"mds", "r", "cli,rest")
COMMAND("mds stop name=who,type=CephString", "stop mds", \
	"mds", "rw", "cli,rest")
COMMAND("mds deactivate name=who,type=CephString", "stop mds", \
	"mds", "rw", "cli,rest")
COMMAND("mds set_max_mds " \
	"name=maxmds,type=CephInt,range=0", \
	"set max MDS index", "mds", "rw", "cli,rest")
COMMAND("mds set " \
	"name=var,type=CephChoices,strings=max_mds|max_file_size"
	"|allow_new_snaps|inline_data|allow_multimds|allow_dirfrags " \
	"name=val,type=CephString "					\
	"name=confirm,type=CephString,req=false",			\
	"set mds parameter <var> to <val>", "mds", "rw", "cli,rest")
// arbitrary limit 0-20 below; worth standing on head to make it
// relate to actual state definitions?
// #include "include/ceph_fs.h"
COMMAND("mds set_state " \
	"name=gid,type=CephInt,range=0 " \
	"name=state,type=CephInt,range=0|20", \
	"set mds state of <gid> to <numeric-state>", "mds", "rw", "cli,rest")
COMMAND("mds fail name=who,type=CephString", \
	"force mds to status failed", "mds", "rw", "cli,rest")
COMMAND("mds repaired name=rank,type=CephString", \
	"mark a damaged MDS rank as no longer damaged", "mds", "rw", "cli,rest")
COMMAND("mds rm " \
	"name=gid,type=CephInt,range=0", \
	"remove nonactive mds", "mds", "rw", "cli,rest")
COMMAND("mds rmfailed name=who,type=CephString name=confirm,type=CephString,req=false", \
	"remove failed mds", "mds", "rw", "cli,rest")
COMMAND("mds cluster_down", "take MDS cluster down", "mds", "rw", "cli,rest")
COMMAND("mds cluster_up", "bring MDS cluster up", "mds", "rw", "cli,rest")
COMMAND("mds compat rm_compat " \
	"name=feature,type=CephInt,range=0", \
	"remove compatible feature", "mds", "rw", "cli,rest")
COMMAND("mds compat rm_incompat " \
	"name=feature,type=CephInt,range=0", \
	"remove incompatible feature", "mds", "rw", "cli,rest")
COMMAND("mds add_data_pool " \
	"name=pool,type=CephString", \
	"add data pool <pool>", "mds", "rw", "cli,rest")
COMMAND("mds remove_data_pool " \
	"name=pool,type=CephString", \
	"remove data pool <pool>", "mds", "rw", "cli,rest")
COMMAND("mds rm_data_pool " \
	"name=pool,type=CephString", \
	"remove data pool <pool>", "mds", "rw", "cli,rest")
COMMAND("mds newfs " \
	"name=metadata,type=CephInt,range=0 " \
	"name=data,type=CephInt,range=0 " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"make new filesystem using pools <metadata> and <data>", \
	"mds", "rw", "cli,rest")
COMMAND("fs new " \
	"name=fs_name,type=CephString " \
	"name=metadata,type=CephString " \
	"name=data,type=CephString ", \
	"make new filesystem using named pools <metadata> and <data>", \
	"fs", "rw", "cli,rest")
COMMAND("fs rm " \
	"name=fs_name,type=CephString " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"disable the named filesystem", \
	"fs", "rw", "cli,rest")
COMMAND("fs reset " \
	"name=fs_name,type=CephString " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"disaster recovery only: reset to a single-MDS map", \
	"fs", "rw", "cli,rest")
COMMAND("fs ls ", \
	"list filesystems", \
	"fs", "r", "cli,rest")
COMMAND("fs get name=fs_name,type=CephString", \
	"get info about one filesystem", \
	"fs", "r", "cli,rest")
COMMAND("fs set " \
	"name=fs_name,type=CephString " \
	"name=var,type=CephChoices,strings=max_mds|max_file_size"
        "|allow_new_snaps|inline_data|cluster_down|allow_multimds|allow_dirfrags " \
	"name=val,type=CephString "					\
	"name=confirm,type=CephString,req=false",			\
	"set mds parameter <var> to <val>", "mds", "rw", "cli,rest")
COMMAND("fs flag set name=flag_name,type=CephChoices,strings=enable_multiple "
        "name=val,type=CephString " \
	"name=confirm,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"Set a global CephFS flag", \
	"fs", "rw", "cli,rest")
COMMAND("fs add_data_pool name=fs_name,type=CephString " \
	"name=pool,type=CephString", \
	"add data pool <pool>", "mds", "rw", "cli,rest")
COMMAND("fs rm_data_pool name=fs_name,type=CephString " \
	"name=pool,type=CephString", \
	"remove data pool <pool>", "mds", "rw", "cli,rest")
COMMAND("fs set_default name=fs_name,type=CephString", \
	"set the default to the named filesystem", \
	"fs", "rw", "cli,rest")

/*
 * Monmap commands
 */
COMMAND("mon dump " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"dump formatted monmap (optionally from epoch)", \
	"mon", "r", "cli,rest")
COMMAND("mon stat", "summarize monitor status", "mon", "r", "cli,rest")
COMMAND("mon getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get monmap", "mon", "r", "cli,rest")
COMMAND("mon add " \
	"name=name,type=CephString " \
	"name=addr,type=CephIPAddr", \
	"add new monitor named <name> at <addr>", "mon", "rw", "cli,rest")
COMMAND("mon remove " \
	"name=name,type=CephString", \
	"remove monitor named <name>", "mon", "rw", "cli,rest")
COMMAND("mon rm " \
	"name=name,type=CephString", \
	"remove monitor named <name>", "mon", "rw", "cli,rest")

/*
 * OSD commands
 */
COMMAND("osd stat", "print summary of OSD map", "osd", "r", "cli,rest")
COMMAND("osd dump " \
	"name=epoch,type=CephInt,range=0,req=false",
	"print summary of OSD map", "osd", "r", "cli,rest")
COMMAND("osd tree " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"print OSD tree", "osd", "r", "cli,rest")
COMMAND("osd ls " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"show all OSD ids", "osd", "r", "cli,rest")
COMMAND("osd getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get OSD map", "osd", "r", "cli,rest")
COMMAND("osd getcrushmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get CRUSH map", "osd", "r", "cli,rest")
COMMAND("osd perf", \
        "print dump of OSD perf summary stats", \
        "osd", \
        "r", \
        "cli,rest")
COMMAND("osd blocked-by", \
	"print histogram of which OSDs are blocking their peers", \
	"osd", "r", "cli,rest")
COMMAND("osd getmaxosd", "show largest OSD id", "osd", "r", "cli,rest")
COMMAND("osd find " \
	"name=id,type=CephInt,range=0", \
	"find osd <id> in the CRUSH map and show its location", \
	"osd", "r", "cli,rest")
COMMAND("osd metadata " \
	"name=id,type=CephInt,range=0,req=false", \
	"fetch metadata for osd {id} (default all)", \
	"osd", "r", "cli,rest")
COMMAND("osd map " \
	"name=pool,type=CephPoolname " \
	"name=object,type=CephObjectname " \
	"name=nspace,type=CephString,req=false", \
	"find pg for <object> in <pool> with [namespace]", "osd", "r", "cli,rest")
COMMAND("osd scrub " \
	"name=who,type=CephString", \
	"initiate scrub on osd <who>", "osd", "rw", "cli,rest")
COMMAND("osd deep-scrub " \
	"name=who,type=CephString", \
	"initiate deep scrub on osd <who>", "osd", "rw", "cli,rest")
COMMAND("osd repair " \
	"name=who,type=CephString", \
	"initiate repair on osd <who>", "osd", "rw", "cli,rest")
COMMAND("osd lspools " \
	"name=auid,type=CephInt,req=false", \
	"list pools", "osd", "r", "cli,rest")
COMMAND("osd blacklist ls", "show blacklisted clients", "osd", "r", "cli,rest")
COMMAND("osd blacklist clear", "clear all blacklisted clients", "osd", "rw",
        "cli,rest")
COMMAND("osd crush rule list", "list crush rules", "osd", "r", "cli,rest")
COMMAND("osd crush rule ls", "list crush rules", "osd", "r", "cli,rest")
COMMAND("osd crush rule dump " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.],req=false", \
	"dump crush rule <name> (default all)", \
	"osd", "r", "cli,rest")
COMMAND("osd crush dump", \
	"dump crush map", \
	"osd", "r", "cli,rest")
COMMAND("osd setcrushmap", "set crush map from input file", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush set", "set crush map from input file", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush add-bucket " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=type,type=CephString", \
	"add no-parent (probably root) crush bucket <name> of type <type>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush rename-bucket " \
	"name=srcname,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=dstname,type=CephString,goodchars=[A-Za-z0-9-_.]", \
	"rename bucket <srcname> to <dstname>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush set " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N,goodchars=[A-Za-z0-9-_.=]", \
	"update crushmap position and weight for <name> to <weight> with location <args>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush add " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N,goodchars=[A-Za-z0-9-_.=]", \
	"add or update crushmap position and weight for <name> with <weight> and location <args>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush create-or-move " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N,goodchars=[A-Za-z0-9-_.=]", \
	"create entry or move existing entry for <name> <weight> at/to location <args>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush move " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=args,type=CephString,n=N,goodchars=[A-Za-z0-9-_.=]", \
	"move existing entry for <name> to location <args>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush link " \
	"name=name,type=CephString " \
	"name=args,type=CephString,n=N,goodchars=[A-Za-z0-9-_.=]", \
	"link existing entry for <name> under location <args>", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush rm " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=ancestor,type=CephString,req=false,goodchars=[A-Za-z0-9-_.]", \
	"remove <name> from crush map (everywhere, or just at <ancestor>)",\
	"osd", "rw", "cli,rest")
COMMAND("osd crush remove " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=ancestor,type=CephString,req=false,goodchars=[A-Za-z0-9-_.]", \
	"remove <name> from crush map (everywhere, or just at <ancestor>)", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush unlink " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=ancestor,type=CephString,req=false,goodchars=[A-Za-z0-9-_.]", \
	"unlink <name> from crush map (everywhere, or just at <ancestor>)", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush reweight-all",
	"recalculate the weights for the tree to ensure they sum correctly",
	"osd", "rw", "cli,rest")
COMMAND("osd crush reweight " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=weight,type=CephFloat,range=0.0", \
	"change <name>'s weight to <weight> in crush map", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush reweight-subtree " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=weight,type=CephFloat,range=0.0", \
	"change all leaf items beneath <name> to <weight> in crush map", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush tunables " \
	"name=profile,type=CephChoices,strings=legacy|argonaut|bobtail|firefly|hammer|jewel|optimal|default", \
	"set crush tunables values to <profile>", "osd", "rw", "cli,rest")
COMMAND("osd crush set-tunable "				    \
	"name=tunable,type=CephChoices,strings=straw_calc_version " \
	"name=value,type=CephInt",
	"set crush tunable <tunable> to <value>",
	"osd", "rw", "cli,rest")
COMMAND("osd crush get-tunable "			      \
	"name=tunable,type=CephChoices,strings=straw_calc_version",
	"get crush tunable <tunable>",
	"osd", "rw", "cli,rest")
COMMAND("osd crush show-tunables", \
	"show current crush tunables", "osd", "r", "cli,rest")
COMMAND("osd crush rule create-simple " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=root,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=type,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=mode,type=CephChoices,strings=firstn|indep,req=false",
	"create crush rule <name> to start from <root>, replicate across buckets of type <type>, using a choose mode of <firstn|indep> (default firstn; indep best for erasure pools)", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush rule create-erasure " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=profile,type=CephString,req=false,goodchars=[A-Za-z0-9-_.=]", \
	"create crush rule <name> for erasure coded pool created with <profile> (default default)", \
	"osd", "rw", "cli,rest")
COMMAND("osd crush rule rm " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] ",	\
	"remove crush rule <name>", "osd", "rw", "cli,rest")
COMMAND("osd crush tree",
	"dump crush buckets and items in a tree view",
	"osd", "r", "cli,rest")
COMMAND("osd setmaxosd " \
	"name=newmax,type=CephInt,range=0", \
	"set new maximum osd value", "osd", "rw", "cli,rest")
COMMAND("osd pause", "pause osd", "osd", "rw", "cli,rest")
COMMAND("osd unpause", "unpause osd", "osd", "rw", "cli,rest")
COMMAND("osd erasure-code-profile set " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.] " \
	"name=profile,type=CephString,n=N,req=false", \
	"create erasure code profile <name> with [<key[=value]> ...] pairs. Add a --force at the end to override an existing profile (VERY DANGEROUS)", \
	"osd", "rw", "cli,rest")
COMMAND("osd erasure-code-profile get " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.]", \
	"get erasure code profile <name>", \
	"osd", "r", "cli,rest")
COMMAND("osd erasure-code-profile rm " \
	"name=name,type=CephString,goodchars=[A-Za-z0-9-_.]", \
	"remove erasure code profile <name>", \
	"osd", "rw", "cli,rest")
COMMAND("osd erasure-code-profile ls", \
	"list all erasure code profiles", \
	"osd", "r", "cli,rest")
COMMAND("osd set " \
	"name=key,type=CephChoices,strings=full|pause|noup|nodown|noout|noin|nobackfill|norebalance|norecover|noscrub|nodeep-scrub|notieragent|sortbitwise|require_jewel_osds", \
	"set <key>", "osd", "rw", "cli,rest")
COMMAND("osd unset " \
	"name=key,type=CephChoices,strings=full|pause|noup|nodown|noout|noin|nobackfill|norebalance|norecover|noscrub|nodeep-scrub|notieragent|sortbitwise", \
	"unset <key>", "osd", "rw", "cli,rest")
COMMAND("osd cluster_snap", "take cluster snapshot (disabled)", \
	"osd", "r", "")
COMMAND("osd down " \
	"type=CephString,name=ids,n=N", \
	"set osd(s) <id> [<id>...] down", "osd", "rw", "cli,rest")
COMMAND("osd out " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] out", "osd", "rw", "cli,rest")
COMMAND("osd in " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] in", "osd", "rw", "cli,rest")
COMMAND("osd rm " \
	"name=ids,type=CephString,n=N", \
	"remove osd(s) <id> [<id>...] in", "osd", "rw", "cli,rest")
COMMAND("osd reweight " \
	"name=id,type=CephInt,range=0 " \
	"type=CephFloat,name=weight,range=0.0|1.0", \
	"reweight osd to 0.0 < <weight> < 1.0", "osd", "rw", "cli,rest")
COMMAND("osd pg-temp " \
	"name=pgid,type=CephPgid " \
	"name=id,type=CephString,n=N,req=false", \
	"set pg_temp mapping pgid:[<id> [<id>...]] (developers only)", \
        "osd", "rw", "cli,rest")
COMMAND("osd primary-temp " \
	"name=pgid,type=CephPgid " \
	"name=id,type=CephString", \
        "set primary_temp mapping pgid:<id>|-1 (developers only)", \
        "osd", "rw", "cli,rest")
COMMAND("osd primary-affinity " \
	"name=id,type=CephOsdName " \
	"type=CephFloat,name=weight,range=0.0|1.0", \
	"adjust osd primary-affinity from 0.0 <= <weight> <= 1.0", \
	"osd", "rw", "cli,rest")
COMMAND("osd lost " \
	"name=id,type=CephInt,range=0 " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"mark osd as permanently lost. THIS DESTROYS DATA IF NO MORE REPLICAS EXIST, BE CAREFUL", \
	"osd", "rw", "cli,rest")
COMMAND("osd create " \
	"name=uuid,type=CephUUID,req=false " \
	"name=id,type=CephInt,range=0,req=false", \
	"create new osd (with optional UUID and ID)", "osd", "rw", "cli,rest")
COMMAND("osd blacklist " \
	"name=blacklistop,type=CephChoices,strings=add|rm " \
	"name=addr,type=CephEntityAddr " \
	"name=expire,type=CephFloat,range=0.0,req=false", \
	"add (optionally until <expire> seconds from now) or remove <addr> from blacklist", \
	"osd", "rw", "cli,rest")
COMMAND("osd pool mksnap " \
	"name=pool,type=CephPoolname " \
	"name=snap,type=CephString", \
	"make snapshot <snap> in <pool>", "osd", "rw", "cli,rest")
COMMAND("osd pool rmsnap " \
	"name=pool,type=CephPoolname " \
	"name=snap,type=CephString", \
	"remove snapshot <snap> from <pool>", "osd", "rw", "cli,rest")
COMMAND("osd pool ls " \
	"name=detail,type=CephChoices,strings=detail,req=false", \
	"list pools", "osd", "r", "cli,rest")
COMMAND("osd pool create " \
	"name=pool,type=CephPoolname " \
	"name=pg_num,type=CephInt,range=0 " \
	"name=pgp_num,type=CephInt,range=0,req=false " \
        "name=pool_type,type=CephChoices,strings=replicated|erasure,req=false " \
	"name=erasure_code_profile,type=CephString,req=false,goodchars=[A-Za-z0-9-_.] " \
	"name=ruleset,type=CephString,req=false " \
        "name=expected_num_objects,type=CephInt,req=false", \
	"create pool", "osd", "rw", "cli,rest")
COMMAND("osd pool delete " \
	"name=pool,type=CephPoolname " \
	"name=pool2,type=CephPoolname,req=false " \
	"name=sure,type=CephChoices,strings=--yes-i-really-really-mean-it,req=false", \
	"delete pool", \
	"osd", "rw", "cli,rest")
COMMAND("osd pool rm " \
	"name=pool,type=CephPoolname " \
	"name=pool2,type=CephPoolname,req=false " \
	"name=sure,type=CephChoices,strings=--yes-i-really-really-mean-it,req=false", \
	"remove pool", \
	"osd", "rw", "cli,rest")
COMMAND("osd pool rename " \
	"name=srcpool,type=CephPoolname " \
	"name=destpool,type=CephPoolname", \
	"rename <srcpool> to <destpool>", "osd", "rw", "cli,rest")
COMMAND("osd pool get " \
	"name=pool,type=CephPoolname " \
	"name=var,type=CephChoices,strings=size|min_size|crash_replay_interval|pg_num|pgp_num|crush_ruleset|hashpspool|nodelete|nopgchange|nosizechange|write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|hit_set_fpp|auid|target_max_objects|target_max_bytes|cache_target_dirty_ratio|cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|erasure_code_profile|min_read_recency_for_promote|all|min_write_recency_for_promote|fast_read|hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority", \
	"get pool parameter <var>", "osd", "r", "cli,rest")
COMMAND("osd pool set " \
	"name=pool,type=CephPoolname " \
	"name=var,type=CephChoices,strings=size|min_size|crash_replay_interval|pg_num|pgp_num|crush_ruleset|hashpspool|nodelete|nopgchange|nosizechange|write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|hit_set_fpp|use_gmt_hitset|debug_fake_ec_pool|target_max_bytes|target_max_objects|cache_target_dirty_ratio|cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|auid|min_read_recency_for_promote|min_write_recency_for_promote|fast_read|hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority " \
	"name=val,type=CephString " \
	"name=force,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"set pool parameter <var> to <val>", "osd", "rw", "cli,rest")
// 'val' is a CephString because it can include a unit.  Perhaps
// there should be a Python type for validation/conversion of strings
// with units.
COMMAND("osd pool set-quota " \
	"name=pool,type=CephPoolname " \
	"name=field,type=CephChoices,strings=max_objects|max_bytes " \
	"name=val,type=CephString",
	"set object or byte limit on pool", "osd", "rw", "cli,rest")
COMMAND("osd pool get-quota " \
        "name=pool,type=CephPoolname ",
        "obtain object or byte limits for pool",
        "osd", "r", "cli,rest")
COMMAND("osd pool stats " \
        "name=name,type=CephString,req=false",
        "obtain stats from all pools, or from specified pool",
        "osd", "r", "cli,rest")
COMMAND("osd utilization",
	"get basic pg distribution stats",
	"osd", "r", "cli,rest")
COMMAND("osd reweight-by-utilization " \
	"name=oload,type=CephInt,req=false " \
	"name=max_change,type=CephFloat,req=false "			\
	"name=max_osds,type=CephInt,req=false "			\
	"name=no_increasing,type=CephChoices,strings=--no-increasing,req=false",\
	"reweight OSDs by utilization [overload-percentage-for-consideration, default 120]", \
	"osd", "rw", "cli,rest")
COMMAND("osd test-reweight-by-utilization " \
	"name=oload,type=CephInt,req=false " \
	"name=max_change,type=CephFloat,req=false "			\
	"name=max_osds,type=CephInt,req=false "			\
	"name=no_increasing,type=CephChoices,strings=--no-increasing,req=false",\
	"dry run of reweight OSDs by utilization [overload-percentage-for-consideration, default 120]", \
	"osd", "rw", "cli,rest")
COMMAND("osd reweight-by-pg " \
	"name=oload,type=CephInt,req=false " \
	"name=max_change,type=CephFloat,req=false "			\
	"name=max_osds,type=CephInt,req=false "			\
	"name=pools,type=CephPoolname,n=N,req=false",			\
	"reweight OSDs by PG distribution [overload-percentage-for-consideration, default 120]", \
	"osd", "rw", "cli,rest")
COMMAND("osd test-reweight-by-pg " \
	"name=oload,type=CephInt,req=false " \
	"name=max_change,type=CephFloat,req=false "			\
	"name=max_osds,type=CephInt,req=false "			\
	"name=pools,type=CephPoolname,n=N,req=false",			\
	"dry run of reweight OSDs by PG distribution [overload-percentage-for-consideration, default 120]", \
	"osd", "rw", "cli,rest")
COMMAND("osd thrash " \
	"name=num_epochs,type=CephInt,range=0", \
	"thrash OSDs for <num_epochs>", "osd", "rw", "cli,rest")
COMMAND("osd df " \
	"name=output_method,type=CephChoices,strings=plain|tree,req=false", \
	"show OSD utilization", "osd", "r", "cli,rest")

// tiering
COMMAND("osd tier add " \
	"name=pool,type=CephPoolname " \
	"name=tierpool,type=CephPoolname " \
	"name=force_nonempty,type=CephChoices,strings=--force-nonempty,req=false",
	"add the tier <tierpool> (the second one) to base pool <pool> (the first one)", \
	"osd", "rw", "cli,rest")
COMMAND("osd tier remove " \
	"name=pool,type=CephPoolname " \
	"name=tierpool,type=CephPoolname",
	"remove the tier <tierpool> (the second one) from base pool <pool> (the first one)", \
	"osd", "rw", "cli,rest")
COMMAND("osd tier rm " \
	"name=pool,type=CephPoolname " \
	"name=tierpool,type=CephPoolname",
	"remove the tier <tierpool> (the second one) from base pool <pool> (the first one)", \
	"osd", "rw", "cli,rest")
COMMAND("osd tier cache-mode " \
	"name=pool,type=CephPoolname " \
	"name=mode,type=CephChoices,strings=none|writeback|forward|readonly|readforward|proxy|readproxy " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it,req=false", \
	"specify the caching mode for cache tier <pool>", "osd", "rw", "cli,rest")
COMMAND("osd tier set-overlay " \
	"name=pool,type=CephPoolname " \
	"name=overlaypool,type=CephPoolname", \
	"set the overlay pool for base pool <pool> to be <overlaypool>", "osd", "rw", "cli,rest")
COMMAND("osd tier remove-overlay " \
	"name=pool,type=CephPoolname ", \
	"remove the overlay pool for base pool <pool>", "osd", "rw", "cli,rest")
COMMAND("osd tier rm-overlay " \
	"name=pool,type=CephPoolname ", \
	"remove the overlay pool for base pool <pool>", "osd", "rw", "cli,rest")

COMMAND("osd tier add-cache " \
	"name=pool,type=CephPoolname " \
	"name=tierpool,type=CephPoolname " \
	"name=size,type=CephInt,range=0", \
	"add a cache <tierpool> (the second one) of size <size> to existing pool <pool> (the first one)", \
	"osd", "rw", "cli,rest")

/*
 * mon/ConfigKeyService.cc
 */

COMMAND("config-key get " \
	"name=key,type=CephString", \
	"get <key>", "config-key", "r", "cli,rest")
COMMAND("config-key put " \
	"name=key,type=CephString " \
	"name=val,type=CephString,req=false", \
	"put <key>, value <val>", "config-key", "rw", "cli,rest")
COMMAND("config-key del " \
	"name=key,type=CephString", \
	"delete <key>", "config-key", "rw", "cli,rest")
COMMAND("config-key rm " \
	"name=key,type=CephString", \
	"rm <key>", "config-key", "rw", "cli,rest")
COMMAND("config-key exists " \
	"name=key,type=CephString", \
	"check for <key>'s existence", "config-key", "r", "cli,rest")
COMMAND("config-key list ", "list keys", "config-key", "r", "cli,rest")
