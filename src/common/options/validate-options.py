#!/usr/bin/env python3

import argparse
import fileinput
import sys
import yaml
from typing import Any, Dict


class ValidationError(Exception):
    pass


OptionType = Dict[str, Any]


def validate_see_also(opt: OptionType, opts: Dict[str, OptionType]) -> None:
    see_also = opt.get('see_also')
    if see_also is None:
        return
    for ref in see_also:
        if ref not in opts:
            msg = f'see_also contains "{ref}". But it is not found.'
            raise ValidationError(msg)


def main() -> None:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('yamls', nargs='*')
    opts = parser.parse_args()
    options = {}
    for yaml_file in opts.yamls:
        with open(yaml_file) as f:
            yml = yaml.load(f, yaml.SafeLoader)
            options.update({opt['name']: opt for opt in yml['options']})
    for name, opt in options.items():
        try:
            validate_see_also(opt, options)
        except ValidationError as e:
            raise Exception(f'failed to validate "{name}": {e}')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
