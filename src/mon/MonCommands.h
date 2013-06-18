// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc. 
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
 * COMMAND(signature, helpstring)
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
 * CephEntityAddr: CephIPAddr + '/nonce'
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
*/

/*
 * pg commands PgMonitor.cc
 */

COMMAND("pg stat", "show placement group status.")
COMMAND("pg getmap", "get binary pg map to -o/stdout")
COMMAND("pg send_pg_creates", "trigger pg creates to be issued")
COMMAND("pg dump " \
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|pools|osds|pgs,n=N,req=false", \
	"show human-readable versions of pg map")
COMMAND("pg dump_json " \
	"name=dumpcontents,type=CephChoices,strings=all|summary|sum|pools|osds|pgs,n=N,req=false", \
	"show human-readable version of pg map in json only")
COMMAND("pg dump_pools_json", "show pg pools info in json only")
COMMAND("pg dump_stuck " \
	"name=stuckops,type=CephChoices,strings=inactive|unclean|stale,n=N,req=false " \
	"name=threshold,type=CephInt,req=false",
	"show information about stuck pgs [--threshold=seconds to consider stuck]")
COMMAND("pg map name=pgid,type=CephPgid", "show mapping of pg to osds")
COMMAND("pg scrub name=pgid,type=CephPgid", "start scrub on <pgid>")
COMMAND("pg deep-scrub name=pgid,type=CephPgid", "start deep-scrub on <pgid>")
COMMAND("pg repair name=pgid,type=CephPgid", "start repair on <pgid>")
COMMAND("pg debug " \
	"name=debugop,type=CephChoices,strings=unfound_objects_exist|degraded_pgs_exist", \
	"show debug info about pgs")
COMMAND("pg force_create_pg name=pgid,type=CephPgid", \
	"force creation of pg <pgid>")
COMMAND("pg set_full_ratio name=ratio,type=CephFloat,range=0.0|1.0", \
	"set ratio at which pgs are considered full")
COMMAND("pg set_nearfull_ratio name=ratio,type=CephFloat,range=0.0|1.0", \
	"set ratio at which pgs are considered nearly full")

/*
 * auth commands AuthMonitor.cc
 */

COMMAND("auth export name=entity,type=CephString,req=false", \
       	"write keyring for requested entity, or master keyring if none given")
COMMAND("auth get name=entity,type=CephString", \
	"write keyring file with requested key")
COMMAND("auth get-key name=entity,type=CephString", "display requested key")
COMMAND("auth print-key name=entity,type=CephString", "display requested key")
COMMAND("auth print_key name=entity,type=CephString", "display requested key")
COMMAND("auth list", "list authentication state")
COMMAND("auth import", "auth import: read keyring file from input")
COMMAND("auth add " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N", \
	"add auth info for <entity> from input file, or random key if no input given, and/or any caps specified in the command")
COMMAND("auth get-or-create-key " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"get, or add, key for <name> from system/caps pairs specified in the command.  If key already exists, any given caps must match the existing caps for that key.")
COMMAND("auth get-or-create " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N,req=false", \
	"add auth info for <entity> from input file, or random key if no input given, and/or any caps specified in the command")
COMMAND("auth caps " \
	"name=entity,type=CephString " \
	"name=caps,type=CephString,n=N", \
	"update caps for <name> from caps specified in the command")
COMMAND("auth del " \
	"name=entity,type=CephString", \
	"delete all caps for <name>")

/*
 * Monitor commands (Monitor.cc)
 */
COMMAND("compact", "cause compaction of monitor's leveldb storage")
COMMAND("fsid", "show cluster FSID/UUID")
COMMAND("log name=logtext,type=CephString,n=N", \
	"log supplied text to the monitor log")
COMMAND("stop_cluster", "DEPRECATED")
COMMAND("injectargs " \
	"name=injected_args,type=CephString,n=N", \
	"inject config arguments into monitor")
COMMAND("status", "show cluster status")
COMMAND("health name=detail,type=CephChoices,strings=detail,req=false", \
	"show cluster health")
COMMAND("df name=detail,type=CephChoices,strings=detail,req=false", \
	"show cluster free space stats")
COMMAND("report name=tags,type=CephString,n=N,req=false", \
	"report full status of cluster, optional title tag strings")
COMMAND("quorum_status", "report status of monitor quorum")
COMMAND("mon_status", "report status of monitors")
COMMAND("sync status", "report status of monitors")
COMMAND("sync force " \
	"name=validate1,type=CephChoices,strings=--yes-i-really-mean-it " \
	"name=validate2,type=CephChoices,strings=--i-know-what-i-am-doing", \
	"force sync of and clear monitor store")
COMMAND("heap " \
	"name=heapcmd,type=CephChoices,strings=dump|start_profiler|stop_profiler|release|stats", \
	"show heap usage info (available only if compiled with tcmalloc)")
COMMAND("quorum name=quorumcmd,type=CephChoices,strings=enter|exit,n=1", \
	"enter or exit quorum")
COMMAND("tell " \
	"name=target,type=CephName " \
	"name=args,type=CephString,n=N", \
	"send a command to a specific daemon")

/*
 * MDS commands (MDSMonitor.cc)
 */

COMMAND("mds stat", "show MDS status")
COMMAND("mds dump " \
	"name=epoch,type=CephInt,req=false,range=0", \
	"dump info, optionally from epoch")
COMMAND("mds getmap " \
	"name=epoch,type=CephInt,req=false,range=0", \
	"get MDS map, optionally from epoch")
COMMAND("mds tell " \
	"name=who,type=CephString " \
	"name=args,type=CephString,n=N", \
	"send command to particular mds")
COMMAND("mds compat show", "show mds compatibility settings")
COMMAND("mds stop name=who,type=CephString", "stop mds")
COMMAND("mds deactivate name=who,type=CephString", "stop mds")
COMMAND("mds set_max_mds " \
	"name=maxmds,type=CephInt,range=0", \
	"set max MDS index")
COMMAND("mds setmap " \
	"name=epoch,type=CephInt,range=0", \
	"set mds map; must supply correct epoch number")
// arbitrary limit 0-20 below; worth standing on head to make it
// relate to actual state definitions?
// #include "include/ceph_fs.h"
COMMAND("mds set_state " \
	"name=gid,type=CephInt,range=0 " \
	"name=state,type=CephInt,range=0|20", \
	"set mds state of <gid> to <numeric-state>")
COMMAND("mds fail name=who,type=CephString", \
	"force mds to status failed")
COMMAND("mds rm " \
	"name=gid,type=CephInt,range=0 " \
	"name=who,type=CephName", \
	"remove nonactive mds")
COMMAND("mds rmfailed name=who,type=CephInt,range=0", "remove failed mds")
COMMAND("mds cluster_down", "take MDS cluster down")
COMMAND("mds cluster_up", "bring MDS cluster up")
COMMAND("mds compat rm_compat " \
	"name=feature,type=CephInt,range=0", \
	"remove compatible feature")
COMMAND("mds compat rm_incompat " \
	"name=feature,type=CephInt,range=0", \
	"remove incompatible feature")
COMMAND("mds add_data_pool " \
	"name=poolid,type=CephInt,range=0", \
	"add data pool <poolid>")
COMMAND("mds remove_data_pool " \
	"name=poolid,type=CephInt,range=0", \
	"remove data pool <poolid>")
COMMAND("mds newfs " \
	"name=metadata,type=CephInt,range=0 " \
	"name=data,type=CephInt,range=0 " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it", \
	"make new filesystom using pools <metadata> and <data>")
/*
 * Monmap commands
 */
COMMAND("mon dump " \
	"name=epoch,type=CephInt,req=false", \
	"dump formatted monmap (optionally from epoch)")
COMMAND("mon stat", "summarize monitor status")
COMMAND("mon getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get monmap")
COMMAND("mon add " \
	"name=name,type=CephString " \
	"name=addr,type=CephIPAddr", \
	"add new monitor named <name> at <addr>")
COMMAND("mon remove " \
	"name=name,type=CephString", \
	"remove monitor named <name>")

/*
 * OSD commands
 */
COMMAND("osd stat", "print summary of OSD map")
COMMAND("osd dump " \
	"name=epoch,type=CephInt,range=0,req=false",
	"print summary of OSD map")
COMMAND("osd tree " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"print OSD tree")
COMMAND("osd ls " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"show all OSD ids")
COMMAND("osd getmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get OSD map")
COMMAND("osd getcrushmap " \
	"name=epoch,type=CephInt,range=0,req=false", \
	"get CRUSH map")
COMMAND("osd getmaxosd", "show largest OSD id")
COMMAND("osd find " \
	"name=id,type=CephInt,range=0", \
	"find osd <id> in the CRUSH map and show its location")
COMMAND("osd map " \
	"name=pool,type=CephPoolname " \
	"name=object,type=CephObjectname", \
	"find pg for <object> in <pool>")
COMMAND("osd scrub " \
	"name=who,type=CephString", \
	"initiate scrub on osd <who>")
COMMAND("osd deep-scrub " \
	"name=who,type=CephString", \
	"initiate deep scrub on osd <who>")
COMMAND("osd repair " \
	"name=who,type=CephString", \
	"initiate repair on osd <who>")
COMMAND("osd lspools " \
	"name=auid,type=CephInt,req=false", \
	"list pools")
COMMAND("osd blacklist ls", "show blacklisted clients")
COMMAND("osd crush rule list", "list crush rules")
COMMAND("osd crush rule ls", "list crush rules")
COMMAND("osd crush rule dump", "dump crush rules")
COMMAND("osd crush dump", "dump crush map")
COMMAND("osd setcrushmap", "set crush map from input file")
COMMAND("osd crush set", "set crush map from input file")
COMMAND("osd crush add-bucket " \
	"name=name,type=CephString " \
	"name=type,type=CephString", \
	"add no-parent (probably root) crush bucket <name> of type <type>")
COMMAND("osd crush set " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N", \
	"set crushmap entry for <name> to <weight> with location <args>")
COMMAND("osd crush add " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N", \
	"add crushmap entry for <name> with <weight> and location <args>")
COMMAND("osd crush create-or-move " \
	"name=id,type=CephOsdName " \
	"name=weight,type=CephFloat,range=0.0 " \
	"name=args,type=CephString,n=N", \
	"create entry or move existing entry for <name> <weight> at/to location <args>")
COMMAND("osd crush move " \
	"name=id,type=CephOsdName " \
	"name=args,type=CephString,n=N", \
	"move existing entry for <name> to location <args>")
COMMAND("osd crush link " \
	"name=name,type=CephString " \
	"name=args,type=CephString,n=N", \
	"link existing entry for <name> under location <args>")
COMMAND("osd crush rm " \
	"name=name,type=CephString", \
	"remove <name> from crush map")
COMMAND("osd crush remove " \
	"name=name,type=CephString", \
	"remove <name> from crush map")
COMMAND("osd crush unlink " \
	"name=name,type=CephString " \
	"name=ancestor,type=CephString,req=false", \
	"unlink <name> from crush map (everywhere, or just at <ancestor>")
COMMAND("osd crush reweight " \
	"name=name,type=CephString " \
	"name=weight,type=CephFloat,range=0.0", \
	"change <name>'s weight to <weight> in crush map")
COMMAND("osd crush tunables " \
	"name=profile,type=CephChoices,strings=legacy|argonaut|bobtail|optimal|default", \
	"set crush tunables values to <profile>")
COMMAND("osd crush rule create-simple " \
	"name=name,type=CephString " \
	"name=root,type=CephString " \
	"name=type,type=CephString", \
	"create crush rule <name> in <root> of type <type>")
COMMAND("osd crush rule rm " \
	"name=name,type=CephString", \
	"remove crush rule <name>")
COMMAND("osd setmaxosd " \
	"name=newmax,type=CephInt,range=0", \
	"set new maximum osd value")
COMMAND("osd pause", "pause osd")
COMMAND("osd unpause", "unpause osd")
COMMAND("osd set " \
	"name=key,type=CephChoices,strings=pause|noup|nodown|noout|noin|nobackfile|norecover", \
	"set <key>")
COMMAND("osd unset " \
	"name=key,type=CephChoices,strings=pause|noup|nodown|noout|noin|nobackfile|norecover", \
	"unset <key>")
COMMAND("osd cluster_snap", "take cluster snapshot (disabled)")
COMMAND("osd down " \
	"type=CephString,name=ids,n=N", \
	"set osd(s) <id> [<id>...] down")
COMMAND("osd out " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] out")
COMMAND("osd in " \
	"name=ids,type=CephString,n=N", \
	"set osd(s) <id> [<id>...] in")
COMMAND("osd rm " \
	"name=ids,type=CephString,n=N", \
	"remove osd(s) <id> [<id>...] in")
COMMAND("osd reweight " \
	"name=id,type=CephInt,range=0 " \
	"type=CephFloat,name=weight,range=0.0|1.0", \
	"reweight osd to 0.0 < <weight> < 1.0")
COMMAND("osd lost " \
	"name=id,type=CephInt,range=0 " \
	"name=sure,type=CephChoices,strings=--yes-i-really-mean-it", \
	"mark osd as permanently lost. THIS DESTROYS DATA IF NO MORE REPLICAS EXIST, BE CAREFUL")
COMMAND("osd create " \
	"name=uuid,type=CephUUID,req=false", \
	"create new osd (with optional UUID)")
COMMAND("osd blacklist " \
	"name=blacklistop,type=CephChoices,strings=add|rm " \
	"name=addr,type=CephEntityAddr " \
	"name=expire,type=CephFloat,range=0.0,req=false", \
	"add (optionally until <expire> seconds from now) or remove <addr> from blacklist")
COMMAND("osd pool mksnap " \
	"name=pool,type=CephPoolname " \
	"name=snap,type=CephString", \
	"make snapshot <snap> in <pool>")
COMMAND("osd pool rmsnap " \
	"name=pool,type=CephPoolname " \
	"name=snap,type=CephString", \
	"remove snapshot <snap> from <pool>")
COMMAND("osd pool create " \
	"name=pool,type=CephPoolname " \
	"name=pg_num,type=CephInt,range=0 " \
	"name=pgp_num,type=CephInt,range=0,req=false", \
	"create pool")
COMMAND("osd pool delete " \
	"name=pool,type=CephPoolname " \
	"name=pool2,type=CephPoolname " \
	"name=sure,type=CephChoices,strings=--yes-i-really-really-mean-it", \
	"delete pool (say pool twice, add --yes-i-really-really-mean-it)")
COMMAND("osd pool rename " \
	"name=srcpool,type=CephPoolname " \
	"name=destpool,type=CephPoolname", \
	"rename <srcpool> to <destpool>")
COMMAND("osd pool get " \
	"name=pool,type=CephPoolname " \
	"name=var,type=CephChoices,strings=size|min_size|crash_replay_interval|pg_num|pgp_num|crush_ruleset", \
	"get pool parameter <var>")
COMMAND("osd pool set " \
	"name=pool,type=CephPoolname " \
	"name=var,type=CephChoices,strings=size|min_size|crash_replay_interval|pg_num|pgp_num|crush_ruleset " \
	"name=val,type=CephInt " \
	"name=sure,type=CephChoices,strings=--allow-experimental-feature,req=false", \
	"set pool parameter <var> to <val>")
// 'val' is a CephString because it can include a unit.  Perhaps
// there should be a Python type for validation/conversion of strings
// with units.
COMMAND("osd pool set-quota " \
	"name=pool,type=CephPoolname " \
	"name=field,type=CephChoices,strings=max_objects|max_bytes " \
	"name=val,type=CephString",
	"set object or byte limit on pool")
COMMAND("osd reweight-by-utilization " \
	"name=oload,type=CephInt,range=100,req=false", \
	"reweight OSDs by utilization [overload-percentage-for-consideration, default 120]")
COMMAND("osd thrash " \
	"name=num_epochs,type=CephInt,range=0", \
	"thrash OSDs for <num_epochs>")

/*
 * mon/ConfigKeyService.cc
 */

COMMAND("config-key get " \
	"name=key,type=CephString", \
	"get <key>")
COMMAND("config-key put " \
	"name=key,type=CephString " \
	"name=val,type=CephString,req=false", \
	"put <key>, value <val>")
COMMAND("config-key del " \
	"name=key,type=CephString", \
	"delete <key>")
COMMAND("config-key exists " \
	"name=key,type=CephString", \
	"check for <key>'s existence")
COMMAND("config-key list ", "list keys")
