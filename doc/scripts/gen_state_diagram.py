#!/usr/bin/env python
import re
import sys

def do_filter(generator):
    return acc_lines(remove_multiline_comments(to_char(remove_single_line_comments(generator))))

def acc_lines(generator):
    current = ""
    for i in generator:
        current += i
        if i == ';' or \
            i == '{' or \
            i == '}':
            yield current.lstrip("\n")
            current = ""

def to_char(generator):
    for line in generator:
        for char in line:
            if char is not '\n':
                yield char
            else:
                yield ' '

def remove_single_line_comments(generator):
    for i in generator:
        if len(i) and i[0] == '#':
            continue
        yield re.sub(r'//.*', '', i)

def remove_multiline_comments(generator):
    saw = ""
    in_comment = False
    for char in generator:
        if in_comment:
            if saw is "*":
                if char is "/":
                    in_comment = False
                saw = ""
            if char is "*":
                saw = "*"
            continue
        if saw is "/":
            if char is '*':
                in_comment = True
                saw = ""
                continue
            else:
                yield saw
                saw = ""
        if char is '/':
            saw = "/"
            continue
        yield char

class StateMachineRenderer(object):
    def __init__(self):
        self.states = {} # state -> parent
        self.machines = {} # state-> initial
        self.edges = {} # event -> [(state, state)]

        self.context = [] # [(context, depth_encountered)]
        self.context_depth = 0
        self.state_contents = {}
        self.subgraphnum = 0
        self.clusterlabel = {}

    def __str__(self):
        return "-------------------\n\nstates: %s\n\n machines: %s\n\n edges: %s\n\n context %s\n\n state_contents %s\n\n--------------------" % (
            self.states,
            self.machines,
            self.edges,
            self.context,
            self.state_contents
            )

    def read_input(self, input_lines):
        for line in input_lines:
            self.get_state(line)
            self.get_event(line)
            self.get_context(line)

    def get_context(self, line):
        match = re.search(r"(\w+::)*::(?P<tag>\w+)::\w+\(const (?P<event>\w+)",
                          line)
        if match is not None:
            self.context.append((match.group('tag'), self.context_depth, match.group('event')))
        if '{' in line:
            self.context_depth += 1
        if '}' in line:
            self.context_depth -= 1
            while len(self.context) and self.context[-1][1] == self.context_depth:
                self.context.pop()

    def get_state(self, line):
        if "boost::statechart::state_machine" in line:
            tokens = re.search(
                r"boost::statechart::state_machine<\s*(\w*),\s*(\w*)\s*>",
                line)
            if tokens is None:
                raise "Error: malformed state_machine line: " + line
            self.machines[tokens.group(1)] = tokens.group(2)
            self.context.append((tokens.group(1), self.context_depth, ""))
            return
        if "boost::statechart::state" in line:
            tokens = re.search(
                r"boost::statechart::state<\s*(\w*),\s*(\w*)\s*,?\s*(\w*)\s*>",
                line)
            if tokens is None:
                raise "Error: malformed state line: " + line
            self.states[tokens.group(1)] = tokens.group(2)
            if tokens.group(2) not in self.state_contents.keys():
                self.state_contents[tokens.group(2)] = []
            self.state_contents[tokens.group(2)].append(tokens.group(1))
            if tokens.group(3) is not "":
                self.machines[tokens.group(1)] = tokens.group(3)
            self.context.append((tokens.group(1), self.context_depth, ""))
            return

    def get_event(self, line):
        if "boost::statechart::transition" in line:
            for i in re.finditer(r'boost::statechart::transition<\s*([\w:]*)\s*,\s*(\w*)\s*>',
                                 line):
                if i.group(1) not in self.edges.keys():
                    self.edges[i.group(1)] = []
                if len(self.context) is 0:
                    raise "no context at line: " + line
                self.edges[i.group(1)].append((self.context[-1][0], i.group(2)))
        i = re.search("return\s+transit<\s*(\w*)\s*>()", line)
        if i is not None:
            if len(self.context) is 0:
                raise "no context at line: " + line
            if self.context[-1][2] is "":
                raise "no event in context at line: " + line
            if self.context[-1][2] not in self.edges.keys():
                self.edges[self.context[-1][2]] = []
            self.edges[self.context[-1][2]].append((self.context[-1][0], i.group(1)))

    def emit_dot(self):
        top_level = []
        for state in self.machines.keys():
            if state not in self.states.keys():
                top_level.append(state)
        print >> sys.stderr, "Top Level States: ", str(top_level)
        print """digraph G {"""
        print '\tsize="7,7"'
        print """\tcompound=true;"""
        for i in self.emit_state(top_level[0]):
            print '\t' + i
        for i in self.edges.keys():
            for j in self.emit_event(i):
                print j
        print """}"""

    def emit_state(self, state):
        if state in self.state_contents.keys():
            self.clusterlabel[state] = "cluster%s" % (str(self.subgraphnum),)
            yield "subgraph cluster%s {" % (str(self.subgraphnum),)
            self.subgraphnum += 1
            yield """\tlabel = "%s";""" % (state,)
            yield """\tcolor = "blue";"""
            for j in self.state_contents[state]:
                for i in self.emit_state(j):
                    yield "\t"+i
            yield "}"
        else:
            found = False
            for (k, v) in self.machines.items():
                if v == state:
                    yield state+"[shape=Mdiamond];"
                    found = True
                    break
            if not found:
                yield state+";"

    def emit_event(self, event):
        def append(app):
            retval = "["
            for i in app:
                retval += (i + ",")
            retval += "]"
            return retval
        for (fro, to) in self.edges[event]:
            appendix = ['label="%s"' % (event,)]
            if fro in self.machines.keys():
                appendix.append("ltail=%s" % (self.clusterlabel[fro],))
                while fro in self.machines.keys():
                    fro = self.machines[fro]
            if to in self.machines.keys():
                appendix.append("lhead=%s" % (self.clusterlabel[to],))
                while to in self.machines.keys():
                    to = self.machines[to]
            yield("%s -> %s %s;" % (fro, to, append(appendix)))



INPUT_GENERATOR = do_filter(sys.stdin.xreadlines())
RENDERER = StateMachineRenderer()
RENDERER.read_input(INPUT_GENERATOR)
RENDERER.emit_dot()
