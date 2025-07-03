#include "ProgressTracker.h"

ProgressTracker::ProgressTracker(const std::string& operation_name) :
  operation_name(operation_name)
{}

void
ProgressTracker::start(uint64_t total_items)
{
  processed_items.store(0);
  this->total_items.store(total_items);
  start_time = std::chrono::steady_clock::now();
  started = true;
  last_displayed_count = 0;
}

void ProgressTracker::start(uint64_t total_items) {
    processed_items.store(0);
    this->total_items.store(total_items);
    start_time = std::chrono::steady_clock::now();
    started = true;
    last_displayed_count = 0;
}

void ProgressTracker::increment(uint64_t count) {
    processed_items.fetch_add(count);
}

void ProgressTracker::set_processed(uint64_t count) {
    processed_items.store(count);
}

void
ProgressTracker::set_total(uint64_t total)
{
  total_items.store(total);
}

void
ProgressTracker::display_progress()
{
  if (!started) {
    return;
  }

  display_progress_internal();
  last_displayed_count = processed_items.load();
}

void
ProgressTracker::display_progress_internal() const
{
  std::lock_guard<std::mutex> lock(display_mutex);

  uint64_t current_processed = get_processed();
  uint64_t current_total = get_total();

  if (current_total > 0) {
    float progress = get_progress_percent();
    int eta_seconds = get_eta_seconds();

    std::string eta_str = (eta_seconds > 0)
                              ? std::to_string(eta_seconds / 60) + "m" +
                                    std::to_string(eta_seconds % 60) + "s"
                              : "calculating";

    std::cout << "\rProcessed " << current_processed << "/" << current_total
              << " objects (" << std::fixed << std::setprecision(2) << progress
              << "%), "
              << "ETA: " << eta_str << std::flush;
  } else {
    std::cout << "\rProcessed " << current_processed << " objects"
              << std::flush;
  }
}

void
ProgressTracker::display_progress_internal() const
{
  std::lock_guard<std::mutex> lock(display_mutex);

  uint64_t current_processed = get_processed();
  uint64_t current_total = get_total();

  if (current_total > 0) {
    float progress = get_progress_percent();
    int eta_seconds = get_eta_seconds();

    std::string eta_str = (eta_seconds > 0)
                              ? std::to_string(eta_seconds / 60) + "m" +
                                    std::to_string(eta_seconds % 60) + "s"
                              : "calculating";

    int64_t remaining = static_cast<int64_t>(current_total) -
                        static_cast<int64_t>(current_processed);
    if (remaining <= 0) {
      return 0;
    }

    return static_cast<int>(remaining / objects_per_sec);
  }