// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#include "Ring.hxx"
#include "system/Error.hxx"

namespace Uring {

Ring::Ring(unsigned entries, unsigned flags)
{
	int error = io_uring_queue_init(entries, &ring, flags);
	if (error < 0)
		throw MakeErrno(-error, "io_uring_queue_init() failed");
}

void
Ring::Submit()
{
	int error = io_uring_submit(&ring);
	if (error < 0)
		throw MakeErrno(-error, "io_uring_submit() failed");
}

struct io_uring_cqe *
Ring::WaitCompletion()
{
	struct io_uring_cqe *cqe;
	int error = io_uring_wait_cqe(&ring, &cqe);
	if (error < 0) {
		if (error == -EAGAIN)
			return nullptr;

		throw MakeErrno(-error, "io_uring_wait_cqe() failed");
	}

	return cqe;
}

struct io_uring_cqe *
Ring::PeekCompletion()
{
	struct io_uring_cqe *cqe;
	int error = io_uring_peek_cqe(&ring, &cqe);
	if (error < 0) {
		if (error == -EAGAIN)
			return nullptr;

		throw MakeErrno(-error, "io_uring_peek_cqe() failed");
	}

	return cqe;
}

} // namespace Uring
