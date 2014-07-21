#!/usr/bin/python
# -*- mode:Python; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#

import argparse
from babeltrace import *
import datetime
import struct
import sys


class Extent(object):
    def __init__(self, offset, length):
        self.offset = offset
        self.length = length
    def __str__(self):
        return str(self.offset) + "+" + str(self.length)
    def __repr__(self):
        return "Extent(" + str(self.offset) + "," + str(self.length) + ")"

class Thread(object):
    def __init__(self, id, threads, window):
        self.id = id
	self.threads = threads
	self.window = window
        self.pendingIO = None
        self.latestIO = None # may not be meaningful
        self.latestCompletion = None # may not be meaningful
        self.lastTS = None
    def insertTS(self, ts):
        if not self.lastTS or ts > self.lastTS:
            self.lastTS = ts
    def issuedIO(self, io):
        latestIOs = []
        for threadID in self.threads:
            thread = self.threads[threadID]
            if thread.latestIO and thread.latestIO.start_time > io.start_time - self.window:
                latestIOs.append(thread.latestIO)
        io.addDependencies(latestIOs)
        self.latestIO = io
    def completedIO(self, io):
        self.latestCompletion = io

def batchUnreachableFrom(deps, base):
    if not base:
        return set()
    if not deps:
        return set()
    unreachable = set()
    searchingFor = set(deps)
    discovered = set()
    boundary = set(base)
    boundaryHorizon = None
    for io in boundary:
        if not boundaryHorizon or io.start_time > boundaryHorizon:
            boundaryHorizon = io.start_time
    searchingHorizon = None
    for io in searchingFor:
        if not searchingHorizon or io.start_time < searchingHorizon:
            searchingHorizon = io.start_time
    tmp = [x for x in searchingFor if boundaryHorizon < x.start_time]
    searchingFor.difference_update(tmp)
    unreachable.update(tmp)
    while boundary and searchingFor:
        io = boundary.pop()
        for dep in io.dependencies:
            if dep in searchingFor:
                searchingFor.remove(dep)
                if dep.start_time == searchingHorizon:
                    searchingHorizon = None
                    for io in searchingFor:
                        if not searchingHorizon or io.start_time < searchingHorizon:
                            searchingHorizon = io.start_time
            if not dep in discovered:
                boundary.add(dep)
        if io.start_time == boundaryHorizon:
            boundaryHorizon = None
            for io in boundary:
                if not boundaryHorizon or io.start_time > boundaryHorizon:
                    boundaryHorizon = io.start_time
            if boundaryHorizon:
                tmp = [x for x in searchingFor if boundaryHorizon < x.start_time]
                searchingFor.difference_update(tmp)
                unreachable.update(tmp)
                searchingHorizon = None
                for io in searchingFor:
                    if not searchingHorizon or io.start_time < searchingHorizon:
                        searchingHorizon = io.start_time
    unreachable.update(searchingFor)
    return unreachable

class IO(object):
    def __init__(self, ionum, start_time, thread, prev):
        self.ionum = ionum
        self.start_time = start_time
        self.thread = thread
        self.dependencies = set()
        self.isCompletion = False
        self.prev = prev
        self.numSuccessors = 0
        self.completion = None
    def reachableFrom(self, ios):
        if not ios:
            return False
        discovered = set()
        boundary = set(ios)
        horizon = None
        for io in boundary:
            if not horizon or io.start_time > horizon:
                horizon = io.start_time
        if horizon < self.start_time:
            return False
        while boundary:
            io = boundary.pop()
            for dep in io.dependencies:
                if self == dep:
                    return True
                if not dep in discovered:
                    boundary.add(dep)
            if io.start_time == horizon:
                horizon = None
                for io in boundary:
                    if not horizon or io.start_time > horizon:
                        horizon = io.start_time
                if horizon and horizon < self.start_time:
                    return False
        return False
    def addDependency(self, dep):
        if not dep.reachableFrom(self.dependencies):
            self.dependencies.add(dep)
    def addDependencies(self, deps):
        base = set(self.dependencies)
        for dep in deps:
            base.update(dep.dependencies)
        unreachable = batchUnreachableFrom(deps, base)
        self.dependencies.update(unreachable)
    def depIDs(self):
        ids = []
        for dep in self.dependencies:
            ids.append(dep.ionum)
        return ids
    def depMap(self):
        deps = dict()
        for dep in self.dependencies:
            deps[dep.ionum] = self.start_time - dep.start_time
        return deps
    def addThreadCompletionDependencies(self, threads, recentCompletions):
        self.addDependencies(recentCompletions)
    def numCompletionSuccessors(self):
        return self.completion.numSuccessors if self.completion else 0
    def writeTo(self, f, iotype):
        f.write(struct.pack("!BIQIII", iotype, self.ionum, self.thread.id, self.numSuccessors, self.numCompletionSuccessors(), len(self.dependencies)))
        for dep in self.dependencies:
            f.write(struct.pack("!IQ", dep.ionum, self.start_time - dep.start_time))

class StartThreadIO(IO):
    def __init__(self, ionum, start_time, thread):
        IO.__init__(self, ionum, start_time, thread, None)
    def writeTo(self, f):
        IO.writeTo(self, f, 0)
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": start thread, thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class StopThreadIO(IO):
    def __init__(self, ionum, start_time, thread):
        IO.__init__(self, ionum, start_time, thread, None)
    def writeTo(self, f):
        IO.writeTo(self, f, 1)
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": stop thread, thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class ReadIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx, extents):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
        self.extents = extents
    def writeTo(self, f):
        IO.writeTo(self, f, 2)
        if len(self.extents) != 1:
            raise ValueError("Expected read to have 1 extent, but it had " + str(len(self.extents)))
        extent = self.extents[0]
        f.write(struct.pack("!QQQ", self.imagectx, extent.offset, extent.length))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": read, extents = " + str(self.extents) + ", thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class WriteIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx, extents):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
        self.extents = extents
    def writeTo(self, f):
        IO.writeTo(self, f, 3)
        if len(self.extents) != 1:
            raise ValueError("Expected read to have 1 extent, but it had " + str(len(self.extents)))
        extent = self.extents[0]
        f.write(struct.pack("!QQQ", self.imagectx, extent.offset, extent.length))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": write, extents = " + str(self.extents) + ", thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class AioReadIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx, extents):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
        self.extents = extents
    def writeTo(self, f):
        IO.writeTo(self, f, 4)
        if len(self.extents) != 1:
            raise ValueError("Expected read to have 1 extent, but it had " + str(len(self.extents)))
        extent = self.extents[0]
        f.write(struct.pack("!QQQ", self.imagectx, extent.offset, extent.length))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": aio read, extents = " + str(self.extents) + ", thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class AioWriteIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx, extents):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
        self.extents = extents
    def writeTo(self, f):
        IO.writeTo(self, f, 5)
        if len(self.extents) != 1:
            raise ValueError("Expected read to have 1 extent, but it had " + str(len(self.extents)))
        extent = self.extents[0]
        f.write(struct.pack("!QQQ", self.imagectx, extent.offset, extent.length))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": aio write, extents = " + str(self.extents) + ", thread = " + str(self.thread.id) + ", deps = " + str(self.depMap()) + ", numSuccessors = " + str(self.numSuccessors) + ", numCompletionSuccessors = " + str(self.numCompletionSuccessors())

class OpenImageIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx, name, snap_name, readonly):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
        self.name = name
        self.snap_name = snap_name
        self.readonly = readonly
    def writeTo(self, f):
        IO.writeTo(self, f, 6)
        f.write(struct.pack("!QI", self.imagectx, len(self.name)))
        f.write(self.name)
        f.write(struct.pack("!I", len(self.snap_name)))
        f.write(self.snap_name)
        f.write(struct.pack("!b", self.readonly))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": open image, thread = " + str(self.thread.id) + ", imagectx = " + str(self.imagectx) + ", name = " + self.name + ", snap_name = " + self.snap_name + ", readonly = " + str(self.readonly) + ", deps = " + str(self.depMap())

class CloseImageIO(IO):
    def __init__(self, ionum, start_time, thread, prev, imagectx):
        IO.__init__(self, ionum, start_time, thread, prev)
        self.imagectx = imagectx
    def writeTo(self, f):
        IO.writeTo(self, f, 7)
        f.write(struct.pack("!Q", self.imagectx))
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": close image, thread = " + str(self.thread.id) + ", imagectx = " + str(self.imagectx) + ", deps = " + str(self.depMap())

class CompletionIO(IO):
    def __init__(self, start_time, thread, baseIO):
        IO.__init__(self, baseIO.ionum + 1, start_time, thread, None)
        self.baseIO = baseIO
        self.isCompletion = True
        self.addDependency(baseIO)
        baseIO.completion = self
    def writeTo(self, f):
        pass
    def __str__(self):
        return str(self.ionum) + ": " + str(self.start_time * 1e-6) + ": completion, thread = " + str(self.thread.id) + ", baseIO = " + str(self.baseIO) + ", deps = " + str(self.depMap())


class Processor(object):
    def __init__(self):
	self.window = 1 * 1e9
	self.threads = {}
	self.ioCount = 0
	self.recentCompletions = []
    def nextID(self):
	val = self.ioCount
	self.ioCount = self.ioCount + 2
	return val
    def completed(self, io):
	self.recentCompletions.append(io)
	self.recentCompletions[:] = [x for x in self.recentCompletions if x.start_time > io.start_time - self.window]
    def run(self, raw_args):
	parser = argparse.ArgumentParser(description='convert librbd trace output to an rbd-replay input file.')
	parser.add_argument('--print-on-read', action="store_true", help='print events as they are read in (for debugging)')
	parser.add_argument('--print-on-write', action="store_true", help='print events as they are written out (for debugging)')
	parser.add_argument('--window', default=1, type=float, help='size of the window, in seconds.  Larger values slow down processing, and smaller values may miss dependencies.  Default: 1')
	parser.add_argument('input', help='trace to read')
	parser.add_argument('output', help='replay file to write')
	args = parser.parse_args(raw_args)
	self.window = 1e9 * args.window
	inputFileName = args.input
	outputFileName = args.output
        ios = []
        pendingIOs = {}
        limit = 100000000000
        printOnRead = args.print_on_read
        printOnWrite = args.print_on_write
        threads = {}
        traces = TraceCollection()
        traces.add_trace(inputFileName, "ctf")

        # Parse phase
        trace_start = None
        count = 0
        for event in traces.events:
            count = count + 1
            if count > limit:
                break
            ts = event.timestamp
            if not trace_start:
                trace_start = ts
            ts = ts - trace_start
            threadID = event["pthread_id"]
            if threadID in threads:
                thread = threads[threadID]
            else:
                thread = Thread(threadID, threads, self.window)
                threads[threadID] = thread
                ionum = self.nextID()
                io = StartThreadIO(ionum, ts, thread)
                ios.append(io)
                if printOnRead:
                    print str(io)
            thread.insertTS(ts)
            if event.name == "librbd:read_enter":
                name = event["name"]
                readid = event["id"]
                imagectx = event["imagectx"]
                ionum = self.nextID()
                thread.pendingIO = ReadIO(ionum, ts, thread, thread.pendingIO, imagectx, [])
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                thread.issuedIO(thread.pendingIO)
                ios.append(thread.pendingIO)
            elif event.name == "librbd:open_image_enter":
                imagectx = event["imagectx"]
                name = event["name"]
                snap_name = event["snap_name"]
                readid = event["id"]
                readonly = event["read_only"]
                ionum = self.nextID()
                thread.pendingIO = OpenImageIO(ionum, ts, thread, thread.pendingIO, imagectx, name, snap_name, readonly)
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                thread.issuedIO(thread.pendingIO)
                ios.append(thread.pendingIO)
            elif event.name == "librbd:open_image_exit":
                thread.pendingIO.end_time = ts
                completionIO = CompletionIO(ts, thread, thread.pendingIO)
                thread.completedIO(completionIO)
                ios.append(completionIO)
                self.completed(completionIO)
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:close_image_enter":
                imagectx = event["imagectx"]
                ionum = self.nextID()
                thread.pendingIO = CloseImageIO(ionum, ts, thread, thread.pendingIO, imagectx)
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                thread.issuedIO(thread.pendingIO)
                ios.append(thread.pendingIO)
            elif event.name == "librbd:close_image_exit":
                thread.pendingIO.end_time = ts
                completionIO = CompletionIO(ts, thread, thread.pendingIO)
                thread.completedIO(completionIO)
                ios.append(completionIO)
                self.completed(completionIO)
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:read_extent":
                offset = event["offset"]
                length = event["length"]
                thread.pendingIO.extents.append(Extent(offset, length))
            elif event.name == "librbd:read_exit":
                thread.pendingIO.end_time = ts
                completionIO = CompletionIO(ts, thread, thread.pendingIO)
                thread.completedIO(completionIO)
                ios.append(completionIO)
                completed(completionIO)
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:write_enter":
                name = event["name"]
                readid = event["id"]
                offset = event["off"]
                length = event["buf_len"]
                imagectx = event["imagectx"]
                ionum = self.nextID()
                thread.pendingIO = WriteIO(ionum, ts, thread, thread.pendingIO, imagectx, [Extent(offset, length)])
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                thread.issuedIO(thread.pendingIO)
                ios.append(thread.pendingIO)
            elif event.name == "librbd:write_exit":
                thread.pendingIO.end_time = ts
                completionIO = CompletionIO(ts, thread, thread.pendingIO)
                thread.completedIO(completionIO)
                ios.append(completionIO)
                completed(completionIO)
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:aio_read_enter":
                name = event["name"]
                readid = event["id"]
                completion = event["completion"]
                imagectx = event["imagectx"]
                ionum = self.nextID()
                thread.pendingIO = AioReadIO(ionum, ts, thread, thread.pendingIO, imagectx, [])
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                ios.append(thread.pendingIO)
                thread.issuedIO(thread.pendingIO)
                pendingIOs[completion] = thread.pendingIO
            elif event.name == "librbd:aio_read_extent":
                offset = event["offset"]
                length = event["length"]
                thread.pendingIO.extents.append(Extent(offset, length))
            elif event.name == "librbd:aio_read_exit":
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:aio_write_enter":
                name = event["name"]
                writeid = event["id"]
                offset = event["off"]
                length = event["len"]
                completion = event["completion"]
                imagectx = event["imagectx"]
                ionum = self.nextID()
                thread.pendingIO = AioWriteIO(ionum, ts, thread, thread.pendingIO, imagectx, [Extent(offset, length)])
                thread.pendingIO.addThreadCompletionDependencies(threads, self.recentCompletions)
                thread.issuedIO(thread.pendingIO)
                ios.append(thread.pendingIO)
                pendingIOs[completion] = thread.pendingIO
                if printOnRead:
                    print str(thread.pendingIO)
            elif event.name == "librbd:aio_complete_enter":
                completion = event["completion"]
                retval = event["rval"]
                if completion in pendingIOs:
                    completedIO = pendingIOs[completion]
                    del pendingIOs[completion]
                    completedIO.end_time = ts
                    completionIO = CompletionIO(ts, thread, completedIO)
                    completedIO.thread.completedIO(completionIO)
                    ios.append(completionIO)
                    self.completed(completionIO)
                    if printOnRead:
                        print str(completionIO)

        # Insert-thread-stop phase
        ios = sorted(ios, key = lambda io: io.start_time)
        for threadID in threads:
            thread = threads[threadID]
            ionum = None
            maxIONum = 0 # only valid if ionum is None
            for io in ios:
                if io.ionum > maxIONum:
                    maxIONum = io.ionum
                if io.start_time > thread.lastTS:
                    ionum = io.ionum
                    if ionum % 2 == 1:
                        ionum = ionum + 1
                    break
            if not ionum:
                if maxIONum % 2 == 1:
                    maxIONum = maxIONum - 1
                ionum = maxIONum + 2
            for io in ios:
                if io.ionum >= ionum:
                    io.ionum = io.ionum + 2
            # TODO: Insert in the right place, don't append and re-sort
            ios.append(StopThreadIO(ionum, thread.lastTS, thread))
            ios = sorted(ios, key = lambda io: io.start_time)

        for io in ios:
            if io.prev and io.prev in io.dependencies:
                io.dependencies.remove(io.prev)
            if io.isCompletion:
                io.dependencies.clear()
            for dep in io.dependencies:
                dep.numSuccessors = dep.numSuccessors + 1

        if printOnRead and printOnWrite:
	    print

        with open(outputFileName, "wb") as f:
            for io in ios:
                if printOnWrite and not io.isCompletion:
                    print str(io)
                io.writeTo(f)

if __name__ == '__main__':
    Processor().run(sys.argv[1:])
