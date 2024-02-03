// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/admin/pg_commands.h"

#include <memory>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <seastar/core/future.hh>

#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/pg.h"

SET_SUBSYS(osd);

using crimson::osd::OSD;
using crimson::osd::PG;
using namespace crimson::common;
using ceph::common::cmd_getval;


namespace crimson::admin::pg {

class PGCommand : public AdminSocketHook {
public:
  // TODO: const correctness of osd
  PGCommand(crimson::osd::OSD& osd,
            std::string_view prefix,
            std::string_view desc,
            std::string_view help)
      : AdminSocketHook{prefix, desc, help}, osd {osd}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
                                      std::string_view format,
                                      ceph::bufferlist&& input) const final
  {
    // we have "ceph tell <pgid> <cmd>". and it is the ceph cli's responsibility
    // to add "pgid" to the cmd dict. as rados_pg_command() does not set it for
    // us. moreover, and "pgid" is not listed in the command description, as user
    // command format does not follow the convention of "<prefix> [<args>,...]"
    // so we have to verify it on the server side.
    std::string pgid_str;
    pg_t pgid;
    if (!cmd_getval(cmdmap, "pgid", pgid_str)) {
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EINVAL, "no pgid specified"});
    } else if (!pgid.parse(pgid_str.c_str())) {
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EINVAL, fmt::format("couldn't parse pgid '{}'", pgid_str)});
    }
    // am i the primary for this pg?
    const auto osdmap = osd.get_shard_services().get_map();
    spg_t spg_id;
    if (!osdmap->get_primary_shard(pgid, &spg_id)) {
      return seastar::make_ready_future<tell_result_t>(tell_result_t{
          -ENOENT, fmt::format("pgid '{}' does not exist", pgid_str)});
    }
    return osd.get_pg_shard_manager().with_pg(
      spg_id,
      [this, spg_id,
       cmdmap=std::move(cmdmap),
       format=std::move(format),
       input=std::move(input)
      ](auto &&pg) mutable {
	if (!pg) {
	  return seastar::make_ready_future<tell_result_t>(tell_result_t{
	      -ENOENT, fmt::format("i don't have pgid '{}'", spg_id)});
	}
	if (!pg->is_primary()) {
	  return seastar::make_ready_future<tell_result_t>(tell_result_t{
	      -EAGAIN, fmt::format("not primary for pgid '{}'", spg_id)});
	}
	return this->do_command(pg, cmdmap, format, std::move(input));
      });
  }

private:
  virtual seastar::future<tell_result_t>
  do_command(Ref<PG> pg,
             const cmdmap_t& cmdmap,
             std::string_view format,
             ceph::bufferlist&& input) const = 0;

  OSD& osd;
};

class QueryCommand final : public PGCommand {
public:
  // TODO: const correctness of osd
  explicit QueryCommand(crimson::osd::OSD& osd) :
    PGCommand{osd,
              "query",
              "",
              "show details of a specific pg"}
  {}
private:
  seastar::future<tell_result_t>
  do_command(Ref<PG> pg,
             const cmdmap_t&,
             std::string_view format,
             ceph::bufferlist&& input) const final
  {
    std::unique_ptr<Formatter> f{Formatter::create(format,
                                                   "json-pretty",
                                                   "json-pretty")};
    f->open_object_section("pg");
    pg->dump_primary(f.get());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }
};

class MarkUnfoundLostCommand final : public PGCommand {
public:
  explicit MarkUnfoundLostCommand(crimson::osd::OSD& osd) :
    PGCommand{osd,
              "mark_unfound_lost",
              "name=pgid,type=CephPgid,req=false"
              " name=mulcmd,type=CephChoices,strings=revert|delete",
              "mark all unfound objects in this pg as lost, either"
              " removing or reverting to a prior version if one is"
              " available"}
  {}
  seastar::future<tell_result_t>
  do_command(Ref<PG> pg,
             const cmdmap_t& cmdmap,
             std::string_view,
             ceph::bufferlist&&) const final
  {
    // what to do with the unfound object specifically.
    std::string cmd;
    int op = -1;
    cmd_getval(cmdmap, "mulcmd", cmd);
    if (cmd == "revert") {
      op = pg_log_entry_t::LOST_REVERT;
    } else if (cmd == "delete") {
      op = pg_log_entry_t::LOST_DELETE;
    } else {
      return seastar::make_ready_future<tell_result_t>(tell_result_t{
        -EINVAL, "mode must be 'revert' or 'delete'; mark not yet implemented"});
    }
    return pg->mark_unfound_lost(op).then([] {
      // TODO
      return seastar::make_ready_future<tell_result_t>();
    });
  }
};

template <bool deep>
class ScrubCommand : public PGCommand {
public:
  explicit ScrubCommand(crimson::osd::OSD& osd) :
    PGCommand{
      osd,
      deep ? "deep_scrub" : "scrub",
      "",
      deep ? "deep scrub pg" : "scrub pg"}
  {}

  seastar::future<tell_result_t>
  do_command(Ref<PG> pg,
	     const cmdmap_t& cmdmap,
	     std::string_view format,
	     ceph::bufferlist&&) const final
  {
    LOG_PREFIX(ScrubCommand::do_command);
    DEBUGDPP("deep: {}", *pg, deep);
    return PG::interruptor::with_interruption([pg] {
      pg->scrubber.handle_scrub_requested(deep);
      return PG::interruptor::now();
    }, [FNAME, pg](std::exception_ptr ep) {
      DEBUGDPP("interrupted with {}", *pg, ep);
    }, pg).then([format] {
      std::unique_ptr<Formatter> f{
	Formatter::create(format, "json-pretty", "json-pretty")
      };
      f->open_object_section("scrub");
      f->dump_bool("deep", deep);
      f->dump_stream("stamp") << ceph_clock_now();
      f->close_section();
      return seastar::make_ready_future<tell_result_t>(std::move(f));
    });
  }
};

} // namespace crimson::admin::pg

namespace crimson::admin {

template <class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args)
{
  return std::make_unique<Hook>(std::forward<Args>(args)...);
}

template std::unique_ptr<AdminSocketHook>
make_asok_hook<crimson::admin::pg::QueryCommand>(crimson::osd::OSD& osd);

template std::unique_ptr<AdminSocketHook>
make_asok_hook<crimson::admin::pg::MarkUnfoundLostCommand>(crimson::osd::OSD& osd);

template std::unique_ptr<AdminSocketHook>
make_asok_hook<crimson::admin::pg::ScrubCommand<true>>(crimson::osd::OSD& osd);
template std::unique_ptr<AdminSocketHook>
make_asok_hook<crimson::admin::pg::ScrubCommand<false>>(crimson::osd::OSD& osd);

} // namespace crimson::admin
