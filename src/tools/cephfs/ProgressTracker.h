#ifndef PROGRESS_TRACKER_H
#define PROGRESS_TRACKER_H

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>

/**
 * Reusable progress tracking utility for cephfs offline tools
 * Provides thread-safe progress tracking with ETA calculation and consistent display formatting
 */
class ProgressTracker {
public:
  /**
     * Constructor
     * @param operation_name Name of the operation being tracked (e.g. "Processing objects")
     */
  explicit ProgressTracker(const std::string& operation_name = "Processing");

  /**
     * Initialize progress tracking
     * @param total_items Total number of items to process (0 if unknown)
     */
  void start(uint64_t total_items = 0);

  /**
     * Update progress by incrementing processed count
     * @param count Number of items processed (default: 1)
     */
  void increment(uint64_t count = 1);

  /**
     * Set the current processed count directly
     * @param count Current number of processed items
     */
  void set_processed(uint64_t count);

  /**
     * Set or update the total number of items
     * @param total Total number of items to process
     */
  void set_total(uint64_t total);

  /**
     * Display current progress (call this periodically)
     * Will only display if enough items have been processed since the last display
     */
  void display_progress() const;

  /**
     * Display final summary when operation is complete
     */
  void display_final_summary() const;

  /**
     * Get the current number of processed items
     */
  uint64_t
  get_processed() const
  {
    return processed_items.load();
  }

  /**
     * Get the total number of items
     */
  uint64_t
  get_total() const
  {
    return total_items.load();
  }

  /**
     * Get progress as a percentage (0.0 to 100.0)
     */
  float get_progress_percent() const;

  /**
     * Get estimated time remaining in seconds
     */
  std::chrono::seconds get_eta_seconds() const;

  /**
     * Configuration methods
     */
  void
  set_operation_name(const std::string& name)
  {
    operation_name = name;
  }

  /**
     * Check if progress tracking has been started
     */
  bool
  is_started() const
  {
    return started;
  }

  void
  set_enable_progress_update(bool val)
  {
    enable_progress_update = val;
  }

private:
  using clock = std::chrono::high_resolution_clock;
  using duration = clock::duration;
  using time_point = clock::time_point;


  /**
   * Display the current progress to stdout.
   *
   * Thread-safety:
   * - Uses an internal mutex to serialize console output across threads.
   * - Reads atomics for processed/total counts.
   *
   * Intended usage:
   * - Called by display_progress() when sufficient progress has been made.
   */
  void display_progress_internal() const;

  /**
   * Build a unique progress event identifier for Ceph mgr progress.
   *
   * Format:
   * - "<operation_name>_<pid>"
   *
   * @return string identifier suitable for Ceph "update_progress_event".
   */
  std::string get_ceph_progress_event() const;

  /**
   * Build a human-readable status line of the current progress.
   *
   * When the total is known (> 0):
   * - "<processed>/<total> objects (<percent>%), ETA: <mm>m<ss>s"
   * When the total is unknown:
   * - "<processed> objects"
   *
   * @return formatted status description for the current progress.
   */
  std::string get_progress_status() const;

  /**
   * Build a final summary string with total duration and average rate.
   *
   * Format:
   * - "<processed> items in <seconds>s (avg: <items/sec> items/sec)"
   *
   * @return formatted completion summary.
   */
  std::string get_completed_status() const;


  /**
   * Push the current progress status to Ceph mgr.
   *
   * Behavior:
   * - No-ops if progress updates are disabled or the 'ceph' binary is not found.
   * - Rate-limited by progress_refresh_interval.
   * - Spawns a child process to call:
   *   ceph mgr cli update_progress_event <event> "<title>: <status>" <percent> --add-to-ceph-s
   *
   * Thread-safety:
   * - Reads atomics for processed/total counts.
   * - Uses internal timestamps for rate limiting.
   *
   * @param message      Human-readable status to include in the event payload.
   * @param force_update If true, bypass the rate limiter and send the update immediately.
  */
  void update_ceph_progress_internal(
      const std::string& message,
      bool force_update = false) const;

  /**
   * Number of items processed so far.
   * Atomic to support updates from multiple threads.
   */
  std::atomic<uint64_t> processed_items{0};

  /**
   * Total number of items to process.
   * 0 indicates an unknown total.
   */
  std::atomic<uint64_t> total_items{0};

  /**
   * Wall-clock time when tracking started.
   * Used to compute elapsed time, throughput, and ETA.
   */
  std::chrono::steady_clock::time_point start_time;

  /**
   * Short description of the tracked operation (e.g., "scan_extents").
   * Appears in console output and Ceph progress events.
   */
  std::string operation_name;

  /**
   * Minimum number of newly processed items before printing progress again.
   * Helps reduce excessive console output.
   */
  int display_interval{100}; // Display progress every N items by default

  /**
   * Indicates whether start() was called and tracking is active.
   */
  bool started{false};

  /**
   * Guards console output to keep status lines consistent across threads.
   */
  mutable std::mutex display_mutex; // For thread-safe console output

  /**
   * The processed count at the last console display.
   * Used to rate-limit display_progress() calls.
   */
  mutable uint64_t last_displayed_count{0};

  /**
   * Timestamp of the last Ceph mgr progress update.
   * Used to throttle external progress event updates.
   */
  mutable time_point last_progress_update{};

  /**
   * Enables sending progress updates to the Ceph mgr.
   * Disabled automatically if prerequisites are not met.
   */
  mutable bool enable_progress_update{false};

  /**
   * Minimum time interval between Ceph mgr progress updates.
   */
  static constexpr duration progress_refresh_interval =
      std::chrono::duration_cast<duration>(std::chrono::seconds(5));

  /**
   * Produce a human readable string from a duration.
   */
  static std::string to_pretty_timedelta(
      std::chrono::duration<uint64_t> duration);
};

#endif // PROGRESS_TRACKER_H
