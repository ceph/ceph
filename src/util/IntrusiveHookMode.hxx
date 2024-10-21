// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

/**
 * Specifies the mode in which a hook for intrusive containers
 * operates.  This is meant to be used as a template argument to the
 * hook class (e.g. #IntrusiveListHook).
 */
enum class IntrusiveHookMode {
	/**
	 * No implicit initialization.
	 */
	NORMAL,

	/**
	 * Keep track of whether the item is currently linked, allows
	 * using method is_linked().  This requires implicit
	 * initialization and requires iterating all items when
	 * deleting them which adds a considerable amount of overhead.
	 */
	TRACK,

	/**
	 * Automatically unlinks the item in the destructor.  This
	 * implies #TRACK and adds code to the destructor.
	 */
	AUTO_UNLINK,
};
