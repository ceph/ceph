#!/usr/bin/python
from __future__ import print_function
from collections import Counter
import argparse
import json
import re
import sys

def gen_mgrs(args, cr_dump):
    """ traverse and return one manager at a time """
    mgrs = cr_dump['coroutine_managers']
    if args.manager is not None:
        yield mgrs[args.manager]
    else:
        for mgr in mgrs:
            yield mgr

def gen_stacks(args, cr_dump):
    """ traverse and return one stack at a time """
    for mgr in gen_mgrs(args, cr_dump):
        for ctx in mgr['run_contexts']:
            for stack in ctx['entries']:
                yield stack

def gen_ops(args, cr_dump):
    """ traverse and return one op at a time """
    for stack in gen_stacks(args, cr_dump):
        for op in stack['ops']:
            yield stack, op

def op_status(op):
    """ return op status or (none) """
    # "status": {"status": "...", "timestamp": "..."}
    return op.get('status', {}).get('status', '(none)')

def do_crs(args, cr_dump):
    """ print a sorted list of coroutines """
    counter = Counter()

    if args.group == 'status':
        print('Count:\tStatus:')
        for _, op in gen_ops(args, cr_dump):
            if args.filter and not re.search(args.filter, op['type']):
                continue
            counter[op_status(op)] += 1
    else:
        print('Count:\tCoroutine:')
        for _, op in gen_ops(args, cr_dump):
            name = op['type']
            if args.filter and not re.search(args.filter, name):
                continue
            counter[name] += 1

    crs = counter.most_common();

    if args.order == 'asc':
        crs.reverse()
    if args.limit:
        crs = crs[:args.limit]

    for op in crs:
        print('%d\t%s' % (op[1], op[0]))
    print('Total:', sum(counter.values()))
    return 0

def match_ops(name, ops):
    """ return true if any op matches the given filter """
    for op in ops:
        if re.search(name, op):
            return True
    return False

def do_stacks(args, cr_dump):
    """ print a list of coroutine stacks """
    print('Stack:\t\tCoroutines:')
    count = 0
    for stack in gen_stacks(args, cr_dump):
        stack_id = stack['stack']
        ops = [op['type'] for op in stack['ops']]
        if args.filter and not match_ops(args.filter, ops):
            continue
        if args.limit and count == args.limit:
            print('...')
            break
        print('%s\t%s' % (stack_id, ', '.join(ops)))
        count += 1
    print('Total:', count)
    return 0

def traverse_spawned_stacks(args, stack, depth, stacks, callback):
    """ recurse through spawned stacks, passing each op to the callback """
    for op in stack['ops']:
        # only filter ops in base stack
        if depth == 0 and args.filter and not re.search(args.filter, op['type']):
            continue
        if not callback(stack, op, depth):
            return False
        for spawned in op.get('spawned', []):
            s = stacks.get(spawned)
            if not s:
                continue
            if not traverse_spawned_stacks(args, s, depth + 1, stacks, callback):
                return False
    return True

def do_stack(args, cr_dump):
    """ inspect a given stack and its descendents """
    # build a lookup table of stacks by id
    stacks = {s['stack']: s for s in gen_stacks(args, cr_dump)}

    stack = stacks.get(args.stack)
    if not stack:
        print('Stack %s not found' % args.stack, file=sys.stderr)
        return 1

    do_stack.count = 0 # for use in closure
    def print_stack_op(stack, op, depth):
        indent = ' ' * depth * 4
        if args.limit and do_stack.count == args.limit:
            print('%s...' % indent)
            return False # stop traversal
        do_stack.count += 1
        print('%s[%s] %s: %s' % (indent, stack['stack'], op['type'], op_status(op)))
        return True

    traverse_spawned_stacks(args, stack, 0, stacks, print_stack_op)
    return 0

def do_spawned(args, cr_dump):
    """ search all ops for the given spawned stack """
    for stack, op in gen_ops(args, cr_dump):
        if args.stack in op.get('spawned', []):
            print('Stack %s spawned by [%s] %s' % (args.stack, stack['stack'], op['type']))
            return 0
    print('Stack %s not spawned' % args.stack, file=sys.stderr)
    return 1

def main():
    parser = argparse.ArgumentParser(description='Parse and inspect the output of the "cr dump" admin socket command.')
    parser.add_argument('--filename', type=argparse.FileType(), default=sys.stdin, help='Input filename (or stdin if empty)')
    parser.add_argument('--filter', type=str, help='Filter by coroutine type (regex syntax is supported)')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--manager', type=int, help='Index into coroutine_managers[]')

    subparsers = parser.add_subparsers()

    crs_parser = subparsers.add_parser('crs', help='Produce a sorted list of coroutines')
    crs_parser.add_argument('--group', type=str, choices=['type', 'status'])
    crs_parser.add_argument('--order', type=str, choices=['desc', 'asc'])
    crs_parser.set_defaults(func=do_crs)

    stacks_parser = subparsers.add_parser('stacks', help='Produce a list of coroutine stacks and their ops')
    stacks_parser.set_defaults(func=do_stacks)

    stack_parser = subparsers.add_parser('stack', help='Inspect a given coroutine stack')
    stack_parser.add_argument('stack', type=str)
    stack_parser.set_defaults(func=do_stack)

    spawned_parser = subparsers.add_parser('spawned', help='Find the op that spawned the given stack')
    spawned_parser.add_argument('stack', type=str)
    spawned_parser.set_defaults(func=do_spawned)

    args = parser.parse_args()
    return args.func(args, json.load(args.filename))

if __name__ == "__main__":
    result = main()
    sys.exit(result)
