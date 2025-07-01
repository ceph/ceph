#!/usr/bin/env python3

import yaml
import argparse
import math
import os
import sys

# flake8: noqa: E127

def type_to_cxx(t):
    return f'Option::TYPE_{t.upper()}'


def level_to_cxx(lv):
    return f'Option::LEVEL_{lv.upper()}'


def eval_str(v):
    if v == "":
        return v
    v = v.strip()
    v = v.strip('"').replace('"', '\\"')
    return f'"{v}"'


def eval_value(v, typ):
    try:
        if typ == 'str':
            return eval_str(v)
        if typ == 'float':
            return float(v)
        if typ in ('uint', 'int', 'size', 'secs', 'millisecs'):
            return int(v)
        if typ == 'bool':
            return 'true' if v else 'false'
        else:
            return f'"{v}"'
    except ValueError:
        times = dict(_min=60,
                     _hr=60*60,
                     _day=24*60*60,
                     _K=1 << 10,
                     _M=1 << 20,
                     _G=1 << 30,
                     _T=1 << 40)
        for unit, m in times.items():
            if v.endswith(unit):
                int(v[:-len(unit)])
                # user defined literals
                return v
        raise ValueError(f'unknown value: {v}')


def set_default(default, typ):
    if default is None:
        return ''
    v = eval_value(default, typ)
    return f'.set_default({v})\n'


def set_daemon_default(default, typ):
    if default is None:
        return ''
    v = eval_value(default, typ)
    return f'.set_daemon_default({v})\n'


def add_tags(tags):
    if tags is None:
        return ''
    cxx = ''
    for tag in tags:
        v = eval_str(tag)
        cxx += f'.add_tag({v})\n'
    return cxx


def add_services(services):
    if services is None:
        return ''
    if len(services) == 1:
        return f'.add_service("{services[0]}")\n'
    else:
        param = ', '.join(f'"{s}"' for s in services)
        return f'.add_service({{{param}}})\n'


def add_see_also(see_also):
    if see_also is None:
        return ''
    param = ', '.join(f'"{v}"' for v in see_also)
    return f'.add_see_also({{{param}}})\n'


def set_desc(desc):
    if desc is None:
        return ''
    v = eval_str(desc)
    return f'.set_description({v})\n'


def set_long_desc(desc):
    if desc is None:
        return ''
    v = eval_str(desc)
    return f'.set_long_description({v})\n'


def set_min_max(mi, ma, typ):
    if mi is None and ma is None:
        return ''
    if mi is not None and ma is not None:
        min_v = eval_value(mi, typ)
        max_v = eval_value(ma, typ)
        if isinstance(min_v, str) and isinstance(max_v, int):
            return f'.set_min_max({min_v}, {max_v}ULL)\n'
        elif isinstance(min_v, int) and isinstance(max_v, str):
            return f'.set_min_max({min_v}ULL, {max_v})\n'
        else:
            return f'.set_min_max({min_v}, {max_v})\n'
    if mi is not None:
        min_v = eval_value(mi, typ)
        return f'.set_min({min_v})\n'
    raise ValueError('set_max() is not implemented')


def set_enum_allowed(values):
    if values is None:
        return ''
    param = ', '.join(f'"{v}"' for v in values)
    return f'.set_enum_allowed({{{param}}})\n'


def add_flags(flags):
    if flags is None:
        return ''
    cxx = ''
    for flag in flags:
        cxx += f'.set_flag(Option::FLAG_{flag.upper()})\n'
    return cxx


def set_validator(validator):
    if validator is None:
        return ''
    validator = validator.rstrip()
    return f'.set_validator({validator})\n'


def add_verbatim(verbatim):
    if verbatim is None:
        return ''
    return verbatim + '\n'


def yaml_to_cxx(opt, indent):
    name = opt['name']
    typ = opt['type']
    ctyp = type_to_cxx(typ)
    level = level_to_cxx(opt['level'])
    cxx = f'Option("{name}", {ctyp}, {level})\n'
    cxx += set_desc(opt.get('desc'))
    cxx += set_long_desc(opt.get('long_desc'))
    cxx += set_default(opt.get('default'), typ)
    cxx += set_daemon_default(opt.get('daemon_default'), typ)
    cxx += set_min_max(opt.get('min'), opt.get('max'), typ)
    cxx += set_enum_allowed(opt.get('enum_values'))
    cxx += set_validator(opt.get('validator'))
    cxx += add_flags(opt.get('flags'))
    cxx += add_services(opt.get('services'))
    cxx += add_tags(opt.get('tags'))
    cxx += add_see_also(opt.get('see_also'))
    verbatim = add_verbatim(opt.get('verbatim'))
    cxx += verbatim
    if verbatim:
        cxx += '\n'
    else:
        cxx = cxx.rstrip()
    cxx += ',\n'
    if indent > 0:
        indented = []
        for line in cxx.split('\n'):
            if line:
                indented.append(' ' * indent + line + '\n')
        cxx = ''.join(indented)
    return cxx


def type_to_h(t):
    if t == 'uint':
        return 'OPT_U32'
    return f'OPT_{t.upper()}'


def yaml_to_h(opt):
    if opt.get('with_legacy', False):
        name = opt['name']
        typ = opt['type']
        htyp = type_to_h(typ)
        return f'OPTION({name}, {htyp})'
    else:
        return ''


TEMPLATE_CC = '''#include "common/options.h"
{headers}

std::vector<Option> get_{name}_options() {{
  return std::vector<Option>({{
@body@
  }});
}}
'''


# PyYAML doesn't check for duplicates even though the YAML spec says
# that mapping keys must be unique and that duplicates must be treated
# as an error.  See https://github.com/yaml/pyyaml/issues/165.
#
# This workaround breaks merge keys -- in "<<: *xyz", duplicate keys
# from xyz mapping raise an error instead of being discarded.
class UniqueKeySafeLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep)
        keys = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in keys:
                raise yaml.constructor.ConstructorError(None, None,
                                                        "found duplicate key",
                                                        key_node.start_mark)
            keys.add(key)
        return mapping


def translate(opts):
    if opts.raw:
        prelude, epilogue = '', ''
    else:
        prelude, epilogue = TEMPLATE_CC.split('@body@')

    if opts.name:
        name = opts.name
    else:
        name = os.path.split(opts.input)[-1]
        name = name.rsplit('.', 1)[0]
    name = name.replace('-', '_')
    # noqa: E127
    with open(opts.input) as infile, \
         open(opts.output, 'w') as cc_file, \
         open(opts.legacy, 'w') as h_file:
        yml = yaml.load(infile, Loader=UniqueKeySafeLoader)
        headers = yml.get('headers', '')
        cc_file.write(prelude.format(name=name, headers=headers))
        options = yml['options']
        for option in options:
            try:
                cc_file.write(yaml_to_cxx(option, opts.indent) + '\n')
                if option.get('with_legacy', False):
                    h_file.write(yaml_to_h(option) + '\n')
            except ValueError as e:
                print(f'failed to translate option "{name}": {e}',
                      file=sys.stderr)
                return 1
        cc_file.write(epilogue.replace("}}", "}"))


def readable_size(value, typ):
    times = dict(T=1 << 40,
                 G=1 << 30,
                 M=1 << 20,
                 K=1 << 10)
    if isinstance(value, str):
        value = value.strip('"')
    try:
        v = int(value)
        if v == 0:
            return 0
        for unit, m in times.items():
            if v % m == 0:
                v = int(v / m)
                return f'{v}_{unit}'
        return v
    except ValueError:
        return value


def readable_duration(value, typ):
    times = dict(day=24*60*60,
                 hr=60*60,
                 min=60)
    if isinstance(value, str):
        value = value.strip('"')
    try:
        v = float(value)
        if math.floor(v) != v:
            return v
        v = int(v)
        if v == 0:
            return 0
        for unit, m in times.items():
            if v % m == 0:
                v = int(v / m)
                return f'{v}_{unit}'
        return v
    except ValueError:
        return value


def readable_millisecs(value, typ):
    return int(value)


def readable(opts):
    with open(opts.input) as infile, open(opts.output, 'w') as outfile:
        yml = yaml.load(infile, Loader=UniqueKeySafeLoader)
        options = yml['options']
        for option in options:
            typ = option['type']
            if typ in ('size', 'uint'):
                do_readable = readable_size
            elif typ in ('float', 'int', 'secs'):
                do_readable = readable_duration
            elif typ == 'millisecs':
                do_readable = readable_millisecs
            else:
                continue
            for field in ['default', 'min', 'max', 'daemon_default']:
                v = option.get(field)
                if v is not None:
                    option[field] = do_readable(v, typ)
        yml['options'] = options
        yaml.dump(yml, outfile, sort_keys=False, indent=2)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--input', dest='input',
                        default='options.yaml',
                        help='the YAML file to be processed')
    parser.add_argument('-o', '--output', dest='output',
                        default='options',
                        help='the path to the generated .cc file')
    parser.add_argument('--legacy', dest='legacy',
                        default='legacy_options',
                        help='the path to the generated legacy .h file')
    parser.add_argument('--indent', type=int,
                        default=4,
                        help='the number of spaces added before each line')
    parser.add_argument('--name',
                        help='the name of the option group')
    parser.add_argument('--raw', action='store_true',
                        help='output the array without the full function')
    parser.add_argument('--op', choices=('readable', 'translate'),
                        default='translate',
                        help='operation to perform.')
    opts = parser.parse_args(sys.argv[1:])
    if opts.op == 'translate':
        translate(opts)
    else:
        readable(opts)


if __name__ == '__main__':
    main()
