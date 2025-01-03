#!/usr/bin/python3

import itertools
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
            if char != '\n':
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
            if saw == "*":
                if char == "/":
                    in_comment = False
                saw = ""
            if char == "*":
                saw = "*"
            continue
        if saw == "/":
            if char == '*':
                in_comment = True
                saw = ""
                continue
            else:
                yield saw
                saw = ""
        if char == '/':
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

        self.color_palette = itertools.cycle([
            "#000000",  # black
            "#1e90ff",  # dodgerblue
            "#ff0000",  # red
            "#0000ff",  # blue
            "#ffa500",  # orange
            "#40e0d0",  # turquoise
            "#c71585",  # mediumvioletred
        ])

    def __str__(self):
        return f'''-------------------

 states: {self.states}

 machines: {self.machines}

 edges: {self.edges}

 context: {self.context}

 state_contents: {self.state_contents}

--------------------'''

    def read_input(self, input_lines):
        previous_line = None
        for line in input_lines:
            self.get_state(line)
            self.get_event(line)
            # pass two lines at a time to get the context so that regexes can
            # match on split signatures
            self.get_context(line, previous_line)
            previous_line = line

    def get_context(self, line, previous_line):
        match = re.search(r"(\w+::)*::(?P<tag>\w+)::\w+\(const (?P<event>\w+)", line)
        if match is None and previous_line is not None:
            # it is possible that we need to match on the previous line as well, so join
            # them to make them one line and try and get this matching
            joined_line = ' '.join([previous_line, line])
            match = re.search(r"(\w+::)*::(?P<tag>\w+)::\w+\(\s*const (?P<event>\w+)", joined_line)
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
                raise Exception("Error: malformed state_machine line: " + line)
            self.machines[tokens.group(1)] = tokens.group(2)
            self.context.append((tokens.group(1), self.context_depth, ""))
            return
        if "boost::statechart::state" in line:
            tokens = re.search(
                r"boost::statechart::state<\s*(\w*),\s*(\w*)\s*,?\s*(\w*)\s*>",
                line)
            if tokens is None:
                raise Exception("Error: malformed state line: " + line)
            self.states[tokens.group(1)] = tokens.group(2)
            if tokens.group(2) not in self.state_contents.keys():
                self.state_contents[tokens.group(2)] = []
            self.state_contents[tokens.group(2)].append(tokens.group(1))
            if tokens.group(3):
                self.machines[tokens.group(1)] = tokens.group(3)
            self.context.append((tokens.group(1), self.context_depth, ""))
            return

    def get_event(self, line):
        if "boost::statechart::transition" in line:
            for i in re.finditer(r'boost::statechart::transition<\s*([\w:]*)\s*,\s*(\w*)\s*>',
                                 line):
                if i.group(1) not in self.edges.keys():
                    self.edges[i.group(1)] = []
                if not self.context:
                    raise Exception("no context at line: " + line)
                self.edges[i.group(1)].append((self.context[-1][0], i.group(2)))
        i = re.search("return\s+transit<\s*(\w*)\s*>()", line)
        if i is not None:
            if not self.context:
                raise Exception("no context at line: " + line)
            if not self.context[-1][2]:
                raise Exception("no event in context at line: " + line)
            if self.context[-1][2] not in self.edges.keys():
                self.edges[self.context[-1][2]] = []
            self.edges[self.context[-1][2]].append((self.context[-1][0], i.group(1)))

    def emit_dot(self, output):
        top_level = []
        for state in self.machines.keys():
            if state not in self.states.keys():
                top_level.append(state)
        print('Top Level States: ', top_level, file=sys.stderr)
        print('digraph G {', file=output)
        print('\tsize="7,7"', file=output)
        print('\tcompound=true;', file=output)
        for i in self.emit_state(top_level[0]):
            print('\t' + i, file=output)
        for i in self.edges.keys():
            for j in self.emit_event(i):
                print(j, file=output)
        print('}', file=output)

    def emit_state(self, state):
        if state in self.state_contents.keys():
            self.clusterlabel[state] = "cluster%s" % (str(self.subgraphnum),)
            yield "subgraph cluster%s {" % (str(self.subgraphnum),)
            self.subgraphnum += 1
            yield """\tlabel = "%s";""" % (state,)
            yield """\tcolor = "black";"""

            if state in self.machines.values():
                yield """\tstyle = "filled";"""
                yield """\tfillcolor = "lightgrey";"""

            for j in self.state_contents[state]:
                for i in self.emit_state(j):
                    yield "\t"+i
            yield "}"
        else:
            found = False
            for (k, v) in self.machines.items():
                if v == state:
                    yield state+"[shape=Mdiamond style=filled fillcolor=lightgrey];"
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
            color = next(self.color_palette)
            appendix = ['label="%s"' % (event,),
                        'color="%s"' % (color,),
                        'fontcolor="%s"' % (color,)]
            if fro in self.machines.keys():
                appendix.append("ltail=%s" % (self.clusterlabel[fro],))
                while fro in self.machines.keys():
                    fro = self.machines[fro]
            if to in self.machines.keys():
                appendix.append("lhead=%s" % (self.clusterlabel[to],))
                while to in self.machines.keys():
                    to = self.machines[to]
            yield("%s -> %s %s;" % (fro, to, append(appendix)))


if __name__ == '__main__':
    INPUT_GENERATOR = do_filter(line for line in sys.stdin)
    RENDERER = StateMachineRenderer()
    RENDERER.read_input(INPUT_GENERATOR)
    RENDERER.emit_dot(output=sys.stdout)
