#include "ProgressTracker.h"

#include <fmt/format.h>

#include <boost/process.hpp>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/cephfs/libcephfs.h"

namespace bp = boost::process;

ProgressTracker::ProgressTracker(const std::string& operation_name) :
  operation_name(operation_name)
{}

void
ProgressTracker::start(uint64_t total)
{
  processed_items.store(0);
  this->total_items.store(total);
  start_time = std::chrono::steady_clock::now();
  started = true;
  last_displayed_count = 0;
}

void
ProgressTracker::increment(uint64_t count)
{
  processed_items.fetch_add(count);
}

void
ProgressTracker::set_processed(uint64_t count)
{
  processed_items.store(count);
}

void
ProgressTracker::set_total(uint64_t total)
{
  total_items.store(total);
}

void
ProgressTracker::display_progress() const
{
  if (!started) {
    return;
  }

  uint64_t current_processed = processed_items.load();
  if (current_processed - last_displayed_count >=
      static_cast<uint64_t>(display_interval)) {
    display_progress_internal();
    last_displayed_count = current_processed;
  }
}

std::string
ProgressTracker::get_ceph_progress_event() const
{
  return fmt::format("{}_{}", operation_name, getpid());
}

std::string
ProgressTracker::get_progress_status() const
{
  uint64_t current_processed = get_processed();
  uint64_t current_total = get_total();
  if (current_total > 0) {
    float progress = get_progress_percent();
    std::chrono::seconds eta_seconds = get_eta_seconds();

    std::string eta_str = (eta_seconds.count() > 0)
                              ? to_pretty_timedelta(eta_seconds)
                              : "calculating";

    return fmt::format(
        "{}/{} objects ({:.2f}%%), ETA: {} (mm:ss)", current_processed,
        current_total, progress, eta_str);
  } else {
    return fmt::format("{} objects", current_processed);
  }
}

std::string
ProgressTracker::get_completed_status() const
{
  auto now = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

  uint64_t final_processed = processed_items.load();
  float avg_rate = duration.count() > 0
                       ? static_cast<float>(final_processed) /
                             static_cast<float>(duration.count())
                       : 0.0f;

  return fmt::format(
      "{} items in {}s (avg: {} items/sec)", final_processed, duration.count(),
      avg_rate);
}

void
ProgressTracker::update_ceph_progress_internal(
    const std::string& message,
    bool force_update) const
{
  if (!enable_progress_update) {
    return;
  }
  const auto ceph_path = bp::search_path("ceph");
  if (ceph_path.empty()) {
    enable_progress_update = false;
    return;
  }

  // Check how long since the last update
  const time_point now = clock::now();
  if (!force_update &&
      (now - last_progress_update) < progress_refresh_interval) {
    return;
  }

  // Set CEPH_CONF from the current runtime configuration if available
  auto env = boost::this_process::environment();
  try {
    const std::string conf_path =
        g_conf().get_conf_path(); // g_conf().get_val<std::string>("conf");
    if (!conf_path.empty()) {
      env["CEPH_CONF"] = conf_path;
    }
  } catch (...) {
    std::cerr << "Warning: Failed to retrieve CEPH_CONF path in "
                 "update_ceph_progress_internal."
              << std::endl;
    // If the config value isn't available, leave CEPH_CONF unchanged
  }

  float progress = get_progress_percent();

  bp::ipstream merged_stream;
  bp::child ceph_cmd(
      ceph_path, "mgr", "cli", "update_progress_event",
      get_ceph_progress_event(), operation_name + ": " + message,
      std::to_string(progress), "--add-to-ceph-s", bp::std_out > merged_stream,
      bp::std_err > merged_stream, env);

  // Capture output lines to display later
  std::string output, line;
  while (ceph_cmd.running() && std::getline(merged_stream, line)) {
    output.append(line).append("\n");
  }

  ceph_cmd.wait();
  last_progress_update = clock::now();
  if (auto ret = ceph_cmd.exit_code()) {
    if (ret == ENOTSUP) {
      static std::once_flag enotsup_msg;
      std::call_once(enotsup_msg, [&output]() {
        std::cerr << output << std::endl;
      });
    } else {
      std::cout << output << std::endl;
      enable_progress_update = false;
    }
  }
}


void
ProgressTracker::display_progress_internal() const
{
  std::lock_guard<std::mutex> lock(display_mutex);

  std::string progress_status = get_progress_status();
  std::cout << "\rProcessed " << progress_status << std::flush;
  update_ceph_progress_internal(progress_status);
}

void
ProgressTracker::display_final_summary() const
{
  if (!started) {
    return;
  }
  std::lock_guard<std::mutex> lock(display_mutex);
  std::string completed_status = get_completed_status();
  std::cout << "\nCompleted! Processed " << completed_status << std::endl;
  update_ceph_progress_internal(completed_status, true);
  update_ceph_progress_internal(get_completed_status(), true);
}

float
ProgressTracker::get_progress_percent() const
{
  uint64_t current_total = total_items.load();
  if (current_total == 0) {
    return 0.0f;
  }

  uint64_t current_processed = processed_items.load();
  return (static_cast<float>(current_processed) /
          static_cast<float>(current_total)) *
         100.0f;
}

std::chrono::seconds
ProgressTracker::get_eta_seconds() const
{
  if (!started) {
    return std::chrono::seconds{0};
  }

  auto now = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

  uint64_t current_processed = processed_items.load();
  uint64_t current_total = total_items.load();

  if (duration.count() == 0 || current_total == 0 || current_processed == 0) {
    return std::chrono::seconds{0};
  }

  // If we've processed more than estimated, ETA is 0 (completing)
  if (current_processed >= current_total) {
    return std::chrono::seconds{0};
  }

  float objects_per_sec = static_cast<float>(current_processed) /
                          static_cast<float>(duration.count());
  if (objects_per_sec <= 0) {
    return std::chrono::seconds{0};
  }

  int64_t remaining = static_cast<int64_t>(current_total) -
                      static_cast<int64_t>(current_processed);
  if (remaining <= 0) {
    return std::chrono::seconds{0};
  }

  return std::chrono::seconds(
      static_cast<uint64_t>(remaining / objects_per_sec));
}

std::string
ProgressTracker::to_pretty_timedelta(std::chrono::duration<uint64_t> duration)
{
  using namespace std::chrono;

  auto duration_seconds = duration_cast<seconds>(duration).count();

  if (duration < seconds{120}) {
    return fmt::format("{}s", duration_seconds);
  }
  if (duration < minutes{120}) {
    return fmt::format("{}m", duration_seconds / 60);
  }
  if (duration < hours{48}) {
    return fmt::format("{}h", duration_seconds / 3600);
  }
  if (duration < hours{24 * 14}) {
    return fmt::format("{}d", duration_seconds / (3600 * 24));
  }
  if (duration < hours{24 * 7 * 12}) {
    return fmt::format("{}w", duration_seconds / (3600 * 24 * 7));
  }
  if (duration < hours{24 * 365 * 2}) {
    return fmt::format("{}M", duration_seconds / (3600 * 24 * 30));
  }
  return fmt::format("{}y", duration_seconds / (3600 * 24 * 365));
}
