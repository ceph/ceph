#include "ProgressTracker.h"

#include <fmt/format.h>
#include <sys/ioctl.h>

#include <string_view>

#include <boost/asio.hpp>
#include <boost/process.hpp>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/cephfs/libcephfs.h"

namespace bp = boost::process;
namespace asio = boost::asio;

namespace {

/**
 * Get the width of the terminal console
 * @return Terminal width in columns, or 80 as default if not available
 */
int get_terminal_width() {
  struct winsize w{0,0,0,0};
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
    return w.ws_col;
  }
  return 80; // Default fallback width
}

/**
 * Output a line to the terminal, padding to full width if stdout is a TTY.
 * This is useful for progress displays that need to overwrite previous output.
 *
 * @param line The text to output (without trailing newline)
 * @param use_carriage_return If true, uses \r instead of \n for line ending
 */
void write_console_line(const std::string& line, bool use_carriage_return = true) {
  static bool is_tty = isatty(fileno(stdout));

  if (is_tty) {
    int terminal_width = get_terminal_width();
    std::string output = line;

    // Truncate if line is longer than terminal width
    if (static_cast<int>(output.length()) > terminal_width - 1) {
      output = output.substr(0, terminal_width - 1);
    }

    // Pad with spaces to fill the entire line
    int padding = terminal_width - static_cast<int>(output.length());
    if (padding > 0) {
      output.append(padding, ' ');
    }

    using namespace std::literals;
    // Use carriage return to overwrite the line, or newline for permanent output
    std::cout << (use_carriage_return ? "\r"sv : ""sv) << output
              << (use_carriage_return ? ""sv : "\n"sv) << std::flush;
  } else {
    // Not a TTY, just output normally with newline
    std::cout << line << std::endl;
  }
}

} // anonymous namespace

ProgressTracker::ProgressTracker(std::string_view operation_name) :
  operation_name(operation_name)
{}

ProgressTracker::~ProgressTracker() {
    display_final_summary();
}

void
ProgressTracker::start(uint64_t total)
{
  processed_items.store(0);
  total_items.store(total);
  start_time = std::chrono::steady_clock::now();
  started = true;
  last_console_update = last_progress_update = clock::now();
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
  const time_point now = clock::now();
  if ((now - last_console_update) < console_refresh_interval) {
      return;
  }
  display_progress_internal();
  last_console_update = clock::now();
}

std::string
ProgressTracker::get_ceph_progress_event() const
{
  return fmt::format("{}_{}", operation_name, getpid());
}

std::string
ProgressTracker::get_progress_status(const float progress) const
{
  uint64_t current_processed = get_processed();
  uint64_t current_total = get_total();
  if (current_total > 0) {
    std::chrono::seconds eta_seconds =
        get_eta_seconds(current_processed, current_total);

    std::string eta_str = (eta_seconds.count() > 0)
                              ? ceph::to_pretty_timedelta(eta_seconds)
                              : "calculating";

    return fmt::format(
        "{}/{} objects ({:.2f}%%), ETA: {}", current_processed,
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
    float progress,
    bool force_update) const
{
  if (!enable_progress_update) {
    return;
  }
  const auto ceph_path = bp::search_path("ceph");
  if (ceph_path.empty()) {
      std::cerr << "Warning: \"ceph\" not found on PATH."
                << std::endl;
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

  try {
    // Create io_context for async operations
    asio::io_context io_context;

    // Async buffer for process output
    std::future<std::string> output_future;
    asio::streambuf output_buffer;

    // Launch process with async pipe
    bp::async_pipe merged_pipe(io_context);

    bp::child ceph_cmd(
        ceph_path, "mgr", "cli", "update_progress_event",
        get_ceph_progress_event(), operation_name + ": " + message,
        std::to_string(progress), "--add-to-ceph-s",
        (bp::std_out & bp::std_err) > merged_pipe,
        env,
        io_context);

    // Asynchronously read all output
    std::string output;
    std::function<void(const boost::system::error_code&, std::size_t)> read_handler;
    read_handler = [&](const boost::system::error_code& ec, std::size_t bytes_transferred) {
      if (!ec && bytes_transferred > 0) {
        std::string chunk(
            asio::buffers_begin(output_buffer.data()),
            asio::buffers_begin(output_buffer.data()) + bytes_transferred);
        output.append(chunk);
        output_buffer.consume(bytes_transferred);

        // Continue reading
        asio::async_read(merged_pipe, output_buffer,
                        asio::transfer_at_least(1), read_handler);
      } else if (ec == asio::error::eof) {
        // End of stream - normal completion
      } else if (ec) {
        // Other error
        std::cerr << "Error reading ceph command output: " << ec.message() << std::endl;
      }
    };

    // Start async read
    asio::async_read(merged_pipe, output_buffer,
                    asio::transfer_at_least(1), read_handler);

    // Run io_context to completion
    io_context.run();

    // Wait for process to complete
    ceph_cmd.wait();

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
  } catch (bp::process_error& ec) {
    std::cerr << ec.what() << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "Exception in update_ceph_progress_internal: " << ex.what() << std::endl;
  }

  last_progress_update = clock::now();
}


void
ProgressTracker::display_progress_internal() const
{
  std::lock_guard<std::mutex> lock(display_mutex);

  float progress = get_progress_percent();

  std::string progress_status = get_progress_status(progress);
  write_console_line(fmt::format("Processed {}", progress_status));
  update_ceph_progress_internal(progress_status, progress);
}

void
ProgressTracker::display_final_summary() const
{
  if (!started) {
    return;
  }
  std::lock_guard<std::mutex> lock(display_mutex);
  std::string completed_status = get_completed_status();
  write_console_line(fmt::format("Completed {}! Processed {}", operation_name, completed_status), false);
  update_ceph_progress_internal(completed_status, 100.0, true);
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
ProgressTracker::get_eta_seconds(
    uint64_t current_processed,
    uint64_t current_total) const
{
  if (!started) {
    return std::chrono::seconds{0};
  }

  auto now = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

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
      static_cast<int64_t>(remaining / objects_per_sec));
}
