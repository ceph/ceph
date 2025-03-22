// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

#include "Cast.hxx"
#include "Concepts.hxx"
#include "IntrusiveHookMode.hxx" // IWYU pragma: export
#include "MemberPointer.hxx"
#include "OptionalCounter.hxx"

#include <iterator>
#include <type_traits>
#include <utility>

/**
 * Template parameter for #IntrusiveList with compile-time options.
 */
struct IntrusiveListOptions {
	/**
	 * @param constant_time_size make size() constant-time by caching the
	 * number of items in a field?
	 */
	bool constant_time_size = false;

	/**
	 * Initialize the list head with nullptr (all zeroes) which
	 * adds some code for checking nullptr, but may reduce the
	 * data section for statically allocated lists.  It's a
	 * trade-off.
	 */
	bool zero_initialized = false;
};

struct IntrusiveListNode {
	IntrusiveListNode *next, *prev;

	static constexpr void Connect(IntrusiveListNode &a,
				      IntrusiveListNode &b) noexcept {
		a.next = &b;
		b.prev = &a;
	}
};

/**
 * @param Tag an arbitrary tag type to allow using multiple base hooks
 */
template<IntrusiveHookMode _mode=IntrusiveHookMode::NORMAL,
	 typename Tag=void>
class IntrusiveListHook {
	template<typename, typename> friend struct IntrusiveListBaseHookTraits;
	template<auto member> friend struct IntrusiveListMemberHookTraits;
	template<typename T, typename HookTraits, IntrusiveListOptions> friend class IntrusiveList;

protected:
	IntrusiveListNode siblings;

public:
	static constexpr IntrusiveHookMode mode = _mode;

	IntrusiveListHook() noexcept {
		if constexpr (mode >= IntrusiveHookMode::TRACK)
			siblings.next = nullptr;
	}

	~IntrusiveListHook() noexcept {
		if constexpr (mode >= IntrusiveHookMode::AUTO_UNLINK)
			if (is_linked())
				unlink();
	}

	IntrusiveListHook(const IntrusiveListHook &) = delete;
	IntrusiveListHook &operator=(const IntrusiveListHook &) = delete;

	void unlink() noexcept {
		IntrusiveListNode::Connect(*siblings.prev, *siblings.next);

		if constexpr (mode >= IntrusiveHookMode::TRACK)
			siblings.next = nullptr;
	}

	bool is_linked() const noexcept
		requires(mode >= IntrusiveHookMode::TRACK) {
		return siblings.next != nullptr;
	}

private:
	static constexpr auto &Cast(IntrusiveListNode &node) noexcept {
		return ContainerCast(node, &IntrusiveListHook::siblings);
	}

	static constexpr const auto &Cast(const IntrusiveListNode &node) noexcept {
		return ContainerCast(node, &IntrusiveListHook::siblings);
	}
};

using SafeLinkIntrusiveListHook =
	IntrusiveListHook<IntrusiveHookMode::TRACK>;
using AutoUnlinkIntrusiveListHook =
	IntrusiveListHook<IntrusiveHookMode::AUTO_UNLINK>;

/**
 * For classes which embed #IntrusiveListHook as base class.
 *
 * @param Tag selector for which #IntrusiveHashSetHook to use
 */
template<typename T, typename Tag=void>
struct IntrusiveListBaseHookTraits {
	/* a never-called helper function which is used by _Cast() */
	template<IntrusiveHookMode mode>
	static constexpr IntrusiveListHook<mode, Tag> _Identity(const IntrusiveListHook<mode, Tag> &) noexcept;

	/* another never-called helper function which "calls"
	   _Identity(), implicitly casting the item to the
	   IntrusiveListHook specialization; we use this to detect
	   which IntrusiveListHook specialization is used */
	template<typename U>
	static constexpr auto _Cast(const U &u) noexcept {
		return decltype(_Identity(u))();
	}

	template<typename U>
	using Hook = decltype(_Cast(std::declval<U>()));

	static constexpr T *Cast(IntrusiveListNode *node) noexcept {
		auto *hook = &Hook<T>::Cast(*node);
		return static_cast<T *>(hook);
	}

	static constexpr auto &ToHook(T &t) noexcept {
		return static_cast<Hook<T> &>(t);
	}
};

/**
 * For classes which embed #IntrusiveListHook as member.
 */
template<auto member>
struct IntrusiveListMemberHookTraits {
	using T = MemberPointerContainerType<decltype(member)>;
	using _Hook = MemberPointerType<decltype(member)>;

	template<typename Dummy>
	using Hook = _Hook;

	static constexpr T *Cast(IntrusiveListNode *node) noexcept {
		auto &hook = Hook<T>::Cast(*node);
		return &ContainerCast(hook, member);
	}

	static constexpr auto &ToHook(T &t) noexcept {
		return t.*member;
	}
};

/**
 * An intrusive doubly-linked circular list.
 */
template<typename T,
	 typename HookTraits=IntrusiveListBaseHookTraits<T>,
	 IntrusiveListOptions options=IntrusiveListOptions{}>
class IntrusiveList {
	static constexpr bool constant_time_size = options.constant_time_size;

	IntrusiveListNode head = options.zero_initialized
		? IntrusiveListNode{nullptr, nullptr}
		: IntrusiveListNode{&head, &head};

	[[no_unique_address]]
	OptionalCounter<constant_time_size> counter;

	static constexpr auto GetHookMode() noexcept {
		return HookTraits::template Hook<T>::mode;
	}

	static constexpr T *Cast(IntrusiveListNode *node) noexcept {
		return HookTraits::Cast(node);
	}

	static constexpr const T *Cast(const IntrusiveListNode *node) noexcept {
		return HookTraits::Cast(const_cast<IntrusiveListNode *>(node));
	}

	static constexpr auto &ToHook(T &t) noexcept {
		return HookTraits::ToHook(t);
	}

	static constexpr const auto &ToHook(const T &t) noexcept {
		return HookTraits::ToHook(const_cast<T &>(t));
	}

	static constexpr IntrusiveListNode &ToNode(T &t) noexcept {
		return ToHook(t).siblings;
	}

	static constexpr const IntrusiveListNode &ToNode(const T &t) noexcept {
		return ToHook(t).siblings;
	}

public:
	using value_type = T;
	using reference = T &;
	using const_reference = const T &;
	using pointer = T *;
	using const_pointer = const T *;
	using size_type = std::size_t;

	constexpr IntrusiveList() noexcept = default;

	IntrusiveList(IntrusiveList &&src) noexcept {
		if (src.empty())
			return;

		head = src.head;
		head.next->prev = &head;
		head.prev->next = &head;

		src.head.next = &src.head;
		src.head.prev = &src.head;

		using std::swap;
		swap(counter, src.counter);
	}

	~IntrusiveList() noexcept {
		if constexpr (GetHookMode() >= IntrusiveHookMode::TRACK)
			clear();
	}

	IntrusiveList &operator=(IntrusiveList &&) = delete;

	friend void swap(IntrusiveList &a, IntrusiveList &b) noexcept {
		using std::swap;

		if (a.empty()) {
			if (b.empty())
				return;

			a.head = b.head;
			a.head.next->prev = &a.head;
			a.head.prev->next = &a.head;

			b.head = {&b.head, &b.head};
		} else if (b.empty()) {
			b.head = a.head;
			b.head.next->prev = &b.head;
			b.head.prev->next = &b.head;

			a.head = {&a.head, &a.head};
		} else {
			swap(a.head, b.head);

			a.head.next->prev = &a.head;
			a.head.prev->next = &a.head;

			b.head.next->prev = &b.head;
			b.head.prev->next = &b.head;
		}

		swap(a.counter, b.counter);
	}

	constexpr bool empty() const noexcept {
		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				return true;

		return head.next == &head;
	}

	constexpr size_type size() const noexcept {
		if constexpr (constant_time_size)
			return counter;
		else
			return std::distance(begin(), end());
	}

	/**
	 * Remove all items from the linked list.
	 */
	void clear() noexcept {
		if constexpr (GetHookMode() >= IntrusiveHookMode::TRACK) {
			/* for SafeLinkIntrusiveListHook, we need to
			   remove each item manually, or else its
			   is_linked() method will not work */
			while (!empty())
				pop_front();
		} else {
			head = {&head, &head};
			counter.reset();
		}
	}

	/**
	 * Like clear(), but invoke a disposer function on each item.
	 *
	 * The disposer is not allowed to destruct the list.
	 */
	void clear_and_dispose(Disposer<value_type> auto disposer) noexcept {
		while (!empty()) {
			disposer(&pop_front());
		}
	}

	/**
	 * Remove all items matching the given predicate and invoke
	 * the given disposer on it.
	 *
	 * Neither the predicate nor the disposer are allowed to
	 * modify the list (or destruct it).
	 *
	 * @return the number of removed items
	 */
	std::size_t remove_and_dispose_if(std::predicate<const_reference> auto pred,
					  Disposer<value_type> auto dispose) noexcept {
		std::size_t result = 0;

		auto *n = head.next;

		while (n != &head) {
			auto *i = Cast(n);
			n = n->next;

			if (pred(*i)) {
				ToHook(*i).unlink();
				--counter;
				dispose(i);
				++result;
			}
		}

		return result;
	}

	const_reference front() const noexcept {
		return *Cast(head.next);
	}

	reference front() noexcept {
		return *Cast(head.next);
	}

	reference pop_front() noexcept {
		auto &i = front();
		ToHook(i).unlink();
		--counter;
		return i;
	}

	void pop_front_and_dispose(Disposer<value_type> auto disposer) noexcept {
		auto &i = pop_front();
		disposer(&i);
	}

	reference back() noexcept {
		return *Cast(head.prev);
	}

	void pop_back() noexcept {
		auto &i = back();
		ToHook(i).unlink();
		--counter;
	}

	class const_iterator;

	class iterator final {
		friend IntrusiveList;
		friend const_iterator;

		IntrusiveListNode *cursor;

		constexpr iterator(IntrusiveListNode *_cursor) noexcept
			:cursor(_cursor) {}

	public:
		using iterator_category = std::bidirectional_iterator_tag;
		using value_type = T;
		using difference_type = std::ptrdiff_t;
		using pointer = value_type *;
		using reference = value_type &;

		iterator() noexcept = default;

		constexpr bool operator==(const iterator &other) const noexcept {
			return cursor == other.cursor;
		}

		constexpr bool operator!=(const iterator &other) const noexcept {
			return !(*this == other);
		}

		constexpr reference operator*() const noexcept {
			return *Cast(cursor);
		}

		constexpr pointer operator->() const noexcept {
			return Cast(cursor);
		}

		auto &operator++() noexcept {
			cursor = cursor->next;
			return *this;
		}

		auto operator++(int) noexcept {
			auto old = *this;
			cursor = cursor->next;
			return old;
		}

		auto &operator--() noexcept {
			cursor = cursor->prev;
			return *this;
		}

		auto operator--(int) noexcept {
			auto old = *this;
			cursor = cursor->prev;
			return old;
		}
	};

	constexpr iterator begin() noexcept {
		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				return end();

		return {head.next};
	}

	constexpr iterator end() noexcept {
		return {&head};
	}

	static constexpr iterator iterator_to(reference t) noexcept {
		return {&ToNode(t)};
	}

	using reverse_iterator = std::reverse_iterator<iterator>;

	constexpr reverse_iterator rbegin() noexcept {
		return reverse_iterator{end()};
	}

	constexpr reverse_iterator rend() noexcept {
		return reverse_iterator{begin()};
	}

	class const_iterator final {
		friend IntrusiveList;

		const IntrusiveListNode *cursor;

		constexpr const_iterator(const IntrusiveListNode *_cursor) noexcept
			:cursor(_cursor) {}

	public:
		using iterator_category = std::bidirectional_iterator_tag;
		using value_type = const T;
		using difference_type = std::ptrdiff_t;
		using pointer = value_type *;
		using reference = value_type &;

		const_iterator() noexcept = default;

		const_iterator(iterator src) noexcept
			:cursor(src.cursor) {}

		constexpr bool operator==(const const_iterator &other) const noexcept {
			return cursor == other.cursor;
		}

		constexpr bool operator!=(const const_iterator &other) const noexcept {
			return !(*this == other);
		}

		constexpr reference operator*() const noexcept {
			return *Cast(cursor);
		}

		constexpr pointer operator->() const noexcept {
			return Cast(cursor);
		}

		auto &operator++() noexcept {
			cursor = cursor->next;
			return *this;
		}

		auto operator++(int) noexcept {
			auto old = *this;
			cursor = cursor->next;
			return old;
		}

		auto &operator--() noexcept {
			cursor = cursor->prev;
			return *this;
		}

		auto operator--(int) noexcept {
			auto old = *this;
			cursor = cursor->prev;
			return old;
		}
	};

	constexpr const_iterator begin() const noexcept {
		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				return end();

		return {head.next};
	}

	constexpr const_iterator end() const noexcept {
		return {&head};
	}

	static constexpr const_iterator iterator_to(const_reference t) noexcept {
		return {&ToNode(t)};
	}

	using const_reverse_iterator = std::reverse_iterator<const_iterator>;

	constexpr const_reverse_iterator rbegin() const noexcept {
		return reverse_iterator{end()};
	}

	constexpr const_reverse_iterator rend() const noexcept {
		return reverse_iterator{begin()};
	}

	/**
	 * @return an iterator to the item following the specified one
	 */
	iterator erase(iterator i) noexcept {
		auto result = std::next(i);
		ToHook(*i).unlink();
		--counter;
		return result;
	}

	/**
	 * @return an iterator to the item following the specified one
	 */
	iterator erase_and_dispose(iterator i,
				   Disposer<value_type> auto disposer) noexcept {
		auto result = erase(i);
		disposer(&*i);
		return result;
	}

	iterator push_front(reference t) noexcept {
		return insert(begin(), t);
	}

	iterator push_back(reference t) noexcept {
		return insert(end(), t);
	}

	/**
	 * Insert a new item before the given position.
	 *
	 * @param p a valid iterator (end() is allowed)for this list
	 * describing the position where to insert
	 *
	 * @return an iterator to the new item
	 */
	iterator insert(iterator p, reference t) noexcept {
		static_assert(!constant_time_size ||
			      GetHookMode() < IntrusiveHookMode::AUTO_UNLINK,
			      "Can't use auto-unlink hooks with constant_time_size");

		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				head = {&head, &head};

		auto &existing_node = *p.cursor;
		auto &new_node = ToNode(t);

		IntrusiveListNode::Connect(*existing_node.prev,
					   new_node);
		IntrusiveListNode::Connect(new_node, existing_node);

		++counter;

		return iterator_to(t);
	}

	/**
	 * Like insert(), but insert after the given position.
	 */
	iterator insert_after(iterator p, reference t) noexcept {
		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				head = {&head, &head};

		return insert(std::next(p), t);
	}

	/**
	 * Move one item of the given list to this one before the
	 * given position.
	 */
	void splice(iterator position,
		    IntrusiveList &from, iterator i) noexcept {
		auto &item = *i;
		from.erase(i);
		insert(position, item);
	}

	/**
	 * Move the given range of items of the given list to this one
	 * before the given position.
	 */
	void splice(iterator position, IntrusiveList &from,
		    iterator _begin, iterator _end, size_type n) noexcept {
		if (_begin == _end)
			return;

		if constexpr (options.zero_initialized)
			if (head.next == nullptr)
				head = {&head, &head};

		auto &next_node = *position.cursor;
		auto &prev_node = *std::prev(position).cursor;

		auto &first_node = *_begin.cursor;
		auto &before_first_node = *std::prev(_begin).cursor;
		auto &last_node = *std::prev(_end).cursor;
		auto &after_last_node = *_end.cursor;

		/* remove from the other list */
		IntrusiveListNode::Connect(before_first_node, after_last_node);
		from.counter -= n;

		/* insert into this list */
		IntrusiveListNode::Connect(prev_node, first_node);
		IntrusiveListNode::Connect(last_node, next_node);
		counter += n;
	}

	/**
	 * Move all items of the given list to this one before the
	 * given position.
	 */
	void splice(iterator position, IntrusiveList &from) noexcept {
		splice(position, from, from.begin(), from.end(),
		       constant_time_size ? from.size() : 1);
	}
};
