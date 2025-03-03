/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2020 Cloudius Systems, Ltd.
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/signal.hh>

/// Seastar apps lib namespace

namespace seastar_apps_lib {


/// \brief Futurized SIGINT/SIGTERM signals handler class
///
/// Seastar-style helper class that allows easy waiting for SIGINT/SIGTERM signals
/// from your app.
///
/// Example:
/// \code
/// #include <seastar/apps/lib/stop_signal.hh>
/// ...
/// int main() {
/// ...
/// seastar::thread th([] {
///    seastar_apps_lib::stop_signal stop_signal;
///    <some code>
///    stop_signal.wait().get();  // this will wait till we receive SIGINT or SIGTERM signal
/// });
/// \endcode
class stop_signal {
    seastar::condition_variable _cond;
    seastar::abort_source _abort_source;

private:
    void on_signal() {
        if (stopping()) {
            return;
        }
        _abort_source.request_abort();
        _cond.broadcast();
    }
public:
    stop_signal() {
        seastar::handle_signal(SIGINT, [this] { on_signal(); });
        seastar::handle_signal(SIGTERM, [this] { on_signal(); });
    }
    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op handler instead.
        seastar::handle_signal(SIGINT, [] {});
        seastar::handle_signal(SIGTERM, [] {});
    }
    seastar::future<> wait() {
        return _cond.wait([this] { return _abort_source.abort_requested(); });
    }
    bool stopping() const {
        return _abort_source.abort_requested();
    }
    auto& abort_source() {
        return _abort_source;
    }
};
}
