/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Dongdong Tao <dongdong.tao@canonical.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <iostream>

// Extract basename from a file path (e.g., "/usr/bin/ceph-osd" -> "ceph-osd")
inline std::string get_basename(const std::string& path) {
    size_t pos = path.find_last_of('/');
    return (pos != std::string::npos) ? path.substr(pos + 1) : path;
}

// Global debug mode flag - shared across all userspace code
// Set via -d command line option
inline bool& get_debug_mode() {
    static bool debug_mode = false;
    return debug_mode;
}

// Debug print function - only prints when debug mode is enabled
template<typename... Args>
void debug_print(Args&&... args) {
    if (!get_debug_mode()) return;
    (std::clog << ... << std::forward<Args>(args));
}

#endif // UTILS_H

