#ifndef PROGRESS_TRACKER_H
#define PROGRESS_TRACKER_H

#include <atomic>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <string>
#include <mutex>

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
     * Will only display if enough items have been processed since last display
     */
    void display_progress();
    
    /**
     * Force display current progress regardless of display interval
     */
    void force_display_progress();
    
    /**
     * Display final summary when operation is complete
     */
    void display_final_summary();
    
    /**
     * Get current number of processed items
     */
    uint64_t get_processed() const { return processed_items.load(); }
    
    /**
     * Get total number of items
     */
    uint64_t get_total() const { return total_items.load(); }
    
    /**
     * Get progress as percentage (0.0 to 100.0)
     */
    float get_progress_percent() const;
    
    /**
     * Get estimated time remaining in seconds
     */
    int get_eta_seconds() const;
    
    /**
     * Get elapsed time in seconds
     */
    int get_elapsed_seconds() const;
    
    /**
     * Get processing rate (items per second)
     */
    float get_processing_rate() const;
    
    /**
     * Configuration methods
     */
    void set_display_interval(int interval) { display_interval = interval; }
    void set_operation_name(const std::string& name) { operation_name = name; }
    
    /**
     * Check if progress tracking has been started
     */
    bool is_started() const { return started; }
    
private:
    void display_progress_internal() const;
    
    std::atomic<uint64_t> processed_items{0};
    std::atomic<uint64_t> total_items{0};
    std::chrono::steady_clock::time_point start_time;
    std::string operation_name;
    int display_interval{100}; // Display progress every N items by default
    bool started{false};
    mutable std::mutex display_mutex; // For thread-safe console output
    
    // Track last display to avoid excessive output
    mutable uint64_t last_displayed_count{0};
};

#endif // PROGRESS_TRACKER_H 