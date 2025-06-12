#include "ProgressTracker.h"

ProgressTracker::ProgressTracker(const std::string& operation_name)
    : operation_name(operation_name) {
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

void ProgressTracker::set_total(uint64_t total) {
    total_items.store(total);
}

void ProgressTracker::display_progress() {
    if (!started) {
        return;
    }

    uint64_t current_processed = processed_items.load();
    if (current_processed - last_displayed_count >= static_cast<uint64_t>(display_interval)) {
        display_progress_internal();
        last_displayed_count = current_processed;
    }
}

void ProgressTracker::force_display_progress() {
    if (!started) {
        return;
    }

    display_progress_internal();
    last_displayed_count = processed_items.load();
}

void ProgressTracker::display_progress_internal() const {
    std::lock_guard<std::mutex> lock(display_mutex);
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
    
    uint64_t current_processed = processed_items.load();
    uint64_t current_total = total_items.load();
    
    if (duration.count() == 0) {
        return;
    }

    float objects_per_sec = static_cast<float>(current_processed) / static_cast<float>(duration.count());
    
    if (current_total > 0) {
        // Handle case where processed exceeds estimated total
        uint64_t effective_total = std::max(current_total, current_processed);
        float progress = static_cast<float>(current_processed) / static_cast<float>(effective_total);
        
        // Calculate ETA safely
        std::string eta_str;
        if (current_processed >= current_total) {
            eta_str = "completing";
        } else {
            int64_t remaining = static_cast<int64_t>(current_total) - static_cast<int64_t>(current_processed);
            if (remaining > 0 && objects_per_sec > 0) {
                int eta_seconds = static_cast<int>(remaining / objects_per_sec);
                eta_str = std::to_string(eta_seconds/60) + "m" + std::to_string(eta_seconds%60) + "s";
            } else {
                eta_str = "calculating";
            }
        }
        
        std::cout << "\rProcessed " << current_processed << "/"
                  << current_total << " objects ("
                  << std::fixed << std::setprecision(2) << (progress * 100.0) << "%), "
                  << "ETA: " << eta_str
                  << std::flush;
    } else {
        std::cout << "\rProcessed " << current_processed << " objects"
                  << std::flush;
    }
}

void ProgressTracker::display_final_summary() {
    if (!started) {
        return;
    }

    std::lock_guard<std::mutex> lock(display_mutex);
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
    
    uint64_t final_processed = processed_items.load();
    float avg_rate = duration.count() > 0 ? 
                    static_cast<float>(final_processed) / static_cast<float>(duration.count()) : 0.0f;
    
    std::cout << "\nCompleted! Processed " << final_processed 
              << " items in " << duration.count() << "s (avg: " 
              << std::fixed << std::setprecision(1) << avg_rate << " items/sec)" << std::endl;
}

float ProgressTracker::get_progress_percent() const {
    uint64_t current_total = total_items.load();
    if (current_total == 0) {
        return 0.0f;
    }
    
    uint64_t current_processed = processed_items.load();
    return (static_cast<float>(current_processed) / static_cast<float>(current_total)) * 100.0f;
}

int ProgressTracker::get_eta_seconds() const {
    if (!started) {
        return 0;
    }
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
    
    uint64_t current_processed = processed_items.load();
    uint64_t current_total = total_items.load();
    
    if (duration.count() == 0 || current_total == 0 || current_processed == 0) {
        return 0;
    }
    
    // If we've processed more than estimated, ETA is 0 (completing)
    if (current_processed >= current_total) {
        return 0;
    }
    
    float objects_per_sec = static_cast<float>(current_processed) / static_cast<float>(duration.count());
    if (objects_per_sec <= 0) {
        return 0;
    }

    int64_t remaining = static_cast<int64_t>(current_total) - static_cast<int64_t>(current_processed);
    if (remaining <= 0) {
        return 0;
    }
    
    return static_cast<int>(remaining / objects_per_sec);
}

int ProgressTracker::get_elapsed_seconds() const {
    if (!started) {
        return 0;
    }
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
    return static_cast<int>(duration.count());
}

float ProgressTracker::get_processing_rate() const {
    if (!started) {
        return 0.0f;
    }
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
    
    if (duration.count() == 0) {
        return 0.0f;
    }
    
    uint64_t current_processed = processed_items.load();
    return static_cast<float>(current_processed) / static_cast<float>(duration.count());
}