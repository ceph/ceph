
/**
 * Copyright (c) 2016 Tino Reichardt
 * - removed unneeded stuff
 * - added win32 compatibility
 *
 * Copyright (c) 2013 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code is derived from software contributed to The NetBSD Foundation
 * by Taylor R. Campbell.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LIST_H
#define LIST_H

#if defined (__cplusplus)
extern "C" {
#endif

#include "memmt.h"

struct list_head {
  struct list_head *prev;
  struct list_head *next;
};

MEM_STATIC void INIT_LIST_HEAD(struct list_head *head)
{
  head->prev = head;
  head->next = head;
}

MEM_STATIC struct list_head *list_first(const struct list_head *head)
{
  return head->next;
}

MEM_STATIC struct list_head *list_last(const struct list_head *head)
{
  return head->prev;
}

MEM_STATIC struct list_head *list_next(const struct list_head *node)
{
  return node->next;
}

MEM_STATIC struct list_head *list_prev(const struct list_head *node)
{
  return node->prev;
}

MEM_STATIC void __list_add_between(struct list_head *prev,
                                   struct list_head *node, struct list_head *next)
{
  prev->next = node;
  node->prev = prev;
  node->next = next;
  next->prev = node;
}

MEM_STATIC void list_add(struct list_head *node, struct list_head *head)
{
  __list_add_between(head, node, head->next);
}

MEM_STATIC void list_add_tail(struct list_head *node, struct list_head *head)
{
  __list_add_between(head->prev, node, head);
}

MEM_STATIC void list_del(struct list_head *entry)
{
  entry->prev->next = entry->next;
  entry->next->prev = entry->prev;
}

MEM_STATIC int list_empty(const struct list_head *head)
{
  return (head->next == head);
}

MEM_STATIC void list_move(struct list_head *node, struct list_head *head)
{
  list_del(node);
  list_add(node, head);
}

MEM_STATIC void list_move_tail(struct list_head *node, struct list_head *head)
{
  list_del(node);
  list_add_tail(node, head);
}

#ifndef CONTAINING_RECORD
#define CONTAINING_RECORD(ptr, type, field) \
	((type*)((char*)(ptr) - (char*)(&((type*)0)->field)))
#endif

#define list_entry(ptr, type, member)	CONTAINING_RECORD(ptr, type, member)
#define	list_for_each(var, head) \
	for ((var) = list_first((head)); (var) != (head); (var) = list_next((var)))

#if defined (__cplusplus)
}
#endif

#endif				/* LIST_H */
