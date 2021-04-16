// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/nvmedevice/nvmedevice.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

static constexpr uint32_t MAX_EVENTS = 1024;

namespace crimson::os::seastore::nvme_device {

/* background io poller for multi-stream write */
void poll_completion(std::vector<::io_context_t>* ctxs, bool* exit) {
  while (*exit == false) {
    for (auto& ctx : *ctxs) {
      io_event events[MAX_EVENTS];

      /*
       * At least a completion should be returned. Otherwise, thread is blocked
       * until it is possible
       */
      int num_events = io_getevents(ctx, 1, MAX_EVENTS, events, NULL);

      for (int i = 0; i < num_events; i++) {
        io_context_t* io_context = (io_context_t*)events[i].obj;
        io_context->done = true;
      }
    }
  }
}

open_ertr::future<>
NormalNBD::open(const std::string &in_path, seastar::open_flags mode) {
  /* Open with posix fd for pass generic NVMe commands */
  fd = seastar::file_desc::open(in_path, (int)mode);
  identify_controller_data_t controller_data = {0, };
  return identify_controller(controller_data).safe_then(
      [this, controller_data, in_path, mode]() {
      protocol_version = controller_data.version;
      logger().debug("nvme protocol {}.{} {}",
          (uint32_t)protocol_version.major_ver,
          (uint32_t)protocol_version.minor_ver,
          (uint32_t)protocol_version.tertiary_ver);

      /*
       * Multi Stream Write
       *
       * When NVMe SSD supports multi stream functionality, it marks oacs bit of
       * identify_controller_data structure (from NVMe Specification 1.4).
       * If oacs field is true, NormalNBD class opens device file multiple times
       * with different stream IDs. When user calls write() with stream argument,
       * NormalNBD finds pre-opened FD with stream ID and submit write IO to the
       * found FD.
       */
      support_multistream = controller_data.oacs.support_directives;
      if (support_multistream) {
        write_life_max = 6;
      }

      open_for_io(in_path, mode);

      /* PWG and PWA are supported from NVMe 1.4 */
      if (protocol_version.major_ver >= 1 && protocol_version.minor_ver >= 4) {
        identify_namespace_data_t namespace_data = {0, };
        identify_namespace(namespace_data).safe_then([this, namespace_data]() {
            /* Revise 0-based value */
            write_granularity = namespace_data.npwg + 1;
            write_alignment = namespace_data.npwa + 1;
            });
      }
      return seastar::now();
      }).handle_error(
        /* If device does not support ioctl, just open without stream */
        crimson::ct_error::input_output_error::handle([this, in_path, mode](auto) {
          open_for_io(in_path, mode);
          return seastar::now();
        }));
}

void
NormalNBD::open_for_io(const std::string &in_path, seastar::open_flags mode)
{
  ctx.resize(write_life_max);
  for (uint32_t i = 0; i < write_life_max; i++) {
    stream_fd.push_back(seastar::file_desc::open(in_path, (int)mode));
    if (i != write_life_not_set) {
      int posix_fd = stream_fd[i].get();
      fcntl(posix_fd, F_SET_FILE_RW_HINT, &i);
    }

    io_setup(MAX_EVENTS, &ctx[i]);
  }
  completion_poller = std::thread(poll_completion, &ctx, &exit);
}

write_ertr::future<>
NormalNBD::write(
  uint64_t offset,
  bufferptr &bptr,
  uint16_t stream) {
  logger().debug(
      "block: do_write offset {} len {}",
      offset,
      bptr.length());
  io_context_t io_context = io_context_t();
  io_prep_pwrite(
      &io_context.cb,
      stream_fd[stream].get(),
      bptr.c_str(),
      bptr.length(),
      offset);
  iocb* cb_ptr[1] = {&io_context.cb};
  io_submit(ctx[stream], 1, cb_ptr);
  return seastar::do_with(std::move(io_context), [] (auto& io_context) {
    /*
     * libaio needs additional poller thread (see poll_completion) to poll IO
     * completion. When the poller catches a completion, it marks "done" field
     * of corresponding io_context.
     */
    if (io_context.done) {
      return seastar::now();
    }
      return seastar::later();
    });
}

read_ertr::future<>
NormalNBD::read(
  uint64_t offset,
  bufferptr &bptr) {
  logger().debug(
      "block: do_read offset {} len {}",
      offset,
      bptr.length());
  io_context_t io_context = io_context_t();
  io_prep_pread(
      &io_context.cb,
      stream_fd[0].get(),
      bptr.c_str(),
      bptr.length(),
      offset);
  iocb* cb_ptr[1] = {&io_context.cb};
  io_submit(ctx[0], 1, cb_ptr);
  return seastar::do_with(std::move(io_context), [] (auto& io_context) {
      if (io_context.done) {
      return seastar::now();
      }
      return seastar::later();
      });
}

seastar::future<>
NormalNBD::close() {
  logger().debug(" close ");
  exit = true;
  completion_poller.join();
  fd.close();
  return seastar::now();
}

nvme_command_ertr::future<>
NormalNBD::pass_through_io(NVMePassThroughCommand& command) {
  logger().debug("block: pass through");
  int ret = fd.ioctl(NVME_IOCTL_IO_CMD, command);
  if (ret < 0) {
    logger().debug("block: pass through failed");
    return crimson::ct_error::input_output_error::make();
  }
  else {
    return nvme_command_ertr::now();
  }
}

nvme_command_ertr::future<>
NormalNBD::identify_namespace(identify_namespace_data_t& namespace_data) {
  nvme_admin_command_t command = {0,};
  command.common_cmd.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
  command.identify_cmd.cns = nvme_identify_command_t::CNS_NAMESPACE;
  command.common_cmd.addr = (uint64_t)&namespace_data;
  command.common_cmd.data_len = sizeof(identify_namespace_data_t);

  return pass_admin(command);
}

nvme_command_ertr::future<>
NormalNBD::identify_controller(identify_controller_data_t& controller_data) {
  nvme_admin_command_t command = {0,};
  command.common_cmd.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
  command.identify_cmd.cns = nvme_identify_command_t::CNS_CONTROLLER;
  command.common_cmd.addr = (uint64_t)&controller_data;
  command.common_cmd.data_len = sizeof(identify_controller_data_t);

  return pass_admin(command);
}

nvme_command_ertr::future<>
NormalNBD::pass_admin(nvme_admin_command_t& command) {
  logger().debug("block: pass admin");
  try {
    int ret = fd.ioctl(NVME_IOCTL_ADMIN_CMD, command);
    if (ret < 0) {
      logger().debug("block: pass admin failed");
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return nvme_command_ertr::now();
    }
  }
  catch (...) {
    logger().debug("block: pass admin failed");
    return crimson::ct_error::input_output_error::make();
  }
}

}
