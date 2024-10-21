// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#pragma once

#include "io/FileDescriptor.hxx"

#include <liburing.h>

namespace Uring {

/**
 * Low-level C++ wrapper for a `struct io_uring`.  It provides simple
 * wrappers to liburing functions and throws std::system_error on
 * errors.
 */
class Ring {
	struct io_uring ring;

public:
	Ring(unsigned entries, unsigned flags);

	~Ring() noexcept {
		io_uring_queue_exit(&ring);
	}

	Ring(const Ring &) = delete;
	Ring &operator=(const Ring &) = delete;

	FileDescriptor GetFileDescriptor() const noexcept {
		return FileDescriptor(ring.ring_fd);
	}

	struct io_uring_sqe *GetSubmitEntry() noexcept {
		return io_uring_get_sqe(&ring);
	}

	void Submit();

	struct io_uring_cqe *WaitCompletion();

	/**
	 * @return a completion queue entry or nullptr on EAGAIN
	 */
	struct io_uring_cqe *PeekCompletion();

	void SeenCompletion(struct io_uring_cqe &cqe) noexcept {
		io_uring_cqe_seen(&ring, &cqe);
	}
};

} // namespace Uring
