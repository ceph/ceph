// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#include "Operation.hxx"
#include "CancellableOperation.hxx"

#include <cassert>
#include <utility>

namespace Uring {

void
Operation::CancelUring() noexcept
{
	if (cancellable == nullptr)
		return;

	std::exchange(cancellable, nullptr)->Cancel(*this);
}

void
Operation::ReplaceUring(Operation &new_operation) noexcept
{
	assert(IsUringPending());

	cancellable->Replace(*this, new_operation);

	assert(cancellable == nullptr);
}

} // namespace Uring
