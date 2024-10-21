// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

#include <concepts>

template<typename F, typename T>
concept Disposer = std::invocable<F, T *>;
