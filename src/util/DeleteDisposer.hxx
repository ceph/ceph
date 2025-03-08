// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

/**
 * A disposer for boost::intrusive that invokes the "delete" operator
 * on the given pointer.
 */
class DeleteDisposer {
public:
	template<typename T>
	void operator()(T *t) const noexcept {
		delete t;
	}
};
