// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#pragma once

#include "Ring.hxx"
#include "util/IntrusiveList.hxx"

#include <liburing.h>

namespace Uring {

class Operation;
class CancellableOperation;

/**
 * High-level C++ wrapper for a `struct io_uring`.  It supports a
 * handler class, cancellation, ...
 */
class Queue {
	Ring ring;

	IntrusiveList<CancellableOperation> operations;

public:
	Queue(unsigned entries, unsigned flags);
	~Queue() noexcept;

	FileDescriptor GetFileDescriptor() const noexcept {
		return ring.GetFileDescriptor();
	}

	struct io_uring_sqe *GetSubmitEntry() noexcept {
		return ring.GetSubmitEntry();
	}

	/**
	 * Like GetSubmitEntry(), but call Submit() if the submit
	 * queue is full.
	 *
	 * May throw exceptions if Submit() fails.
	 */
	struct io_uring_sqe &RequireSubmitEntry();

	bool HasPending() const noexcept {
		return !operations.empty();
	}

protected:
	void AddPending(struct io_uring_sqe &sqe,
			Operation &operation) noexcept;

public:
	void Push(struct io_uring_sqe &sqe,
		  Operation &operation) noexcept {
		AddPending(sqe, operation);
		Submit();
	}

	virtual void Submit() {
		ring.Submit();
	}

	bool DispatchOneCompletion();

	void DispatchCompletions() {
		while (DispatchOneCompletion()) {}
	}

	bool WaitDispatchOneCompletion();

	void WaitDispatchCompletions() {
		while (WaitDispatchOneCompletion()) {}
	}

private:
	void DispatchOneCompletion(struct io_uring_cqe &cqe) noexcept;
};

} // namespace Uring
