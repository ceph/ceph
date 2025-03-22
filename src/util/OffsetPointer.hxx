// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

#include <cstddef>

/**
 * Offset the given pointer by the specified number of bytes.
 */
constexpr void *
OffsetPointer(void *p, std::ptrdiff_t offset) noexcept
{
	return static_cast<std::byte *>(p) + offset;
}

/**
 * Offset the given pointer by the specified number of bytes.
 */
constexpr const void *
OffsetPointer(const void *p, std::ptrdiff_t offset) noexcept
{
	return static_cast<const std::byte *>(p) + offset;
}
