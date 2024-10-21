// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#pragma once

namespace Uring {

class CancellableOperation;

/**
 * An asynchronous I/O operation to be queued in a #Queue instance.
 */
class Operation {
	friend class CancellableOperation;

	CancellableOperation *cancellable = nullptr;

public:
	Operation() noexcept = default;

	~Operation() noexcept {
		CancelUring();
	}

	Operation(const Operation &) = delete;
	Operation &operator=(const Operation &) = delete;

	/**
	 * Are we waiting for the operation to complete?
	 */
	bool IsUringPending() const noexcept {
		return cancellable != nullptr;
	}

	/**
	 * Cancel the operation.  OnUringCompletion() will not be
	 * invoked.  This is a no-op if none is pending.
	 */
	void CancelUring() noexcept;

	/**
	 * Replace this pending operation with a new one.  This method
	 * is only legal if IsUringPending().
	 */
	void ReplaceUring(Operation &new_operation) noexcept;

	/**
	 * This method is called when the operation completes.
	 *
	 * @param res the result code; the meaning is specific to the
	 * operation, but negative values usually mean an error has
	 * occurred
	 */
	virtual void OnUringCompletion(int res) noexcept = 0;
};

} // namespace Uring
