// SPDX-License-Identifier: BSD-2-Clause
// Copyright CM4all GmbH
// author: Max Kellermann <mk@cm4all.com>

#pragma once

template<typename T>
struct MemberPointerHelper;

template<typename C, typename M>
struct MemberPointerHelper<M C::*> {
	using ContainerType = C;
	using MemberType = M;
};

/**
 * Given a member pointer, this determines the member type.
 */
template<typename T>
using MemberPointerType =
	typename MemberPointerHelper<T>::MemberType;

/**
 * Given a member pointer, this determines the container type.
 */
template<typename T>
using MemberPointerContainerType =
	typename MemberPointerHelper<T>::ContainerType;
