// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "alien_admin_socket.h"
#include "crimson/admin/admin_socket.h"
#include <seastar/core/alien.hh>

using string_prefix = std::string;

static const std::string alien_prefix = "alien ";

// Inheriting from string to preserve initialization order:
// 1) prefix_str
// 2) AdminSocketHook
// We need to store prefix_str, since AdminSocketHook is directly storing string_view (no copy)
class CnAdminSocketHook : public string_prefix, public crimson::admin::AdminSocketHook
{
 public:
  CnAdminSocketHook(std::string prefix_str,
                    std::string_view desc,
                    std::string_view help,
                    seastar::alien::instance& inst,
                    unsigned shard,
                    std::unique_ptr<crimson::os::ThreadPool>& tp,
                    ::AdminSocketHook* hook)
    :string_prefix(prefix_str)
    ,crimson::admin::AdminSocketHook(*static_cast<string_prefix*>(this), desc, help)
    ,inst(inst)
    ,shard(shard)
    ,tp(tp)
    ,hook(hook)
  {
  }
  ~CnAdminSocketHook()
  {
  }
 private:
  seastar::alien::instance& inst;
  unsigned shard;
  std::unique_ptr<crimson::os::ThreadPool>& tp;
  ::AdminSocketHook* hook;

  seastar::future<crimson::admin::tell_result_t> call(
    const cmdmap_t& cmdmap,
    std::string_view format,
    ceph::bufferlist&& input) const override
  {
    return seastar::do_with(
      std::stringstream{},
      ceph::buffer::list{},
      [=] (std::stringstream& errss, auto& out) {
        return tp->submit([this, cmdmap = cmdmap, format, &errss, &out] {
        std::unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
        int res = hook->call(
          (*static_cast<const string_prefix*>(this)).substr(alien_prefix.length()),
          cmdmap, f.get(), errss, out);
          if (out.length() == 0) {
            f->flush(out);
          }
          return res;
        }).then([&] (int res) {
          return seastar::make_ready_future<crimson::admin::tell_result_t>(res, errss.str(), std::move(out));
        });
      });
  };
};

CnAdminSocket::CnAdminSocket(
  ceph::common::CephContext* cct,
  seastar::lw_shared_ptr<crimson::admin::AdminSocket> cn_asok,
  std::unique_ptr<crimson::os::ThreadPool>& tp,
  seastar::alien::instance& inst,
  unsigned shard)
:AdminSocket(cct)
,cn_asok(cn_asok)
,tp(tp)
,inst(inst)
,shard(shard)
{  
}

CnAdminSocket::~CnAdminSocket()
{
  seastar::alien::submit_to(inst, shard, [this] {
    // deleting admin_socket causes waiting on threads
    cn_asok = nullptr;
    return seastar::make_ready_future<>();
  }).wait();
}

int CnAdminSocket::register_command(
  std::string_view cmddesc,
  AdminSocketHook *hook,
  std::string_view help)
{
  std::string_view prefix;
  std::string_view desc;
  // splitting cmddesc(classic) into prefix and desc (crimson)
  size_t pos = cmddesc.find('=');
  if (pos != std::string_view::npos) {
    pos = cmddesc.rfind(' ', pos);
    ceph_assert(pos != std::string_view::npos);
    prefix = cmddesc.substr(0, pos);
    desc = cmddesc.substr(pos + 1);
  } else {
    prefix = cmddesc;
    desc = "";
  }
  CnAdminSocketHook* cn_hook = new CnAdminSocketHook(
    alien_prefix + std::string(prefix), desc, help, inst, shard, tp, hook);
  seastar::alien::submit_to(inst, shard, [this, hook, cn_hook] {
    cn_asok->register_command(std::unique_ptr<CnAdminSocketHook>(cn_hook));
    hook_tracker[hook].push_back(cn_hook);
    return seastar::make_ready_future<>();
  }).wait();
  return 0;
}

void CnAdminSocket::unregister_commands(
  const AdminSocketHook *hook)
{
  seastar::alien::submit_to(inst, shard, [this, &hook] {
    auto it = hook_tracker.find(hook);
    if (it != hook_tracker.end()) {
      for (auto& i : it->second) {
        cn_asok->unregister_command(i->prefix);
      }
    }
    return seastar::make_ready_future<>();
  }).wait();
}
