"""CLI module for smb ctl support lib."""

import typing
from typing import Any

import argparse
import configparser
import functools
import json
import logging
import logging.config
import os
import pathlib
import sys

from ._probe import protobuf_choose_impl

# control the backend implementation of protobuf when using older versions
# this avoids needing to document PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION
# for the user of this tool.
protobuf_choose_impl()

from .client import APICallError, Client  # noqa: E402
from .config import ChannelType, Config, TLSPath  # noqa: E402

logger = logging.getLogger()

DEFAULT_SOCK_SEARCH = '/var/lib/ceph:/srv/ceph'


class DestinationError(ValueError):
    def __init__(
        self, *args: Any, hints: typing.Optional[typing.List[str]] = None
    ) -> None:
        super().__init__(*args)
        self.hints = hints


class Commands:
    """CLI Commands registry."""

    def __init__(self) -> None:
        self._commands: dict[str, dict] = {}
        self._arguments: dict[str, typing.Callable] = {}

    def register(self, name: str) -> typing.Callable:
        def _arguments(func: typing.Callable) -> typing.Callable:
            self._arguments[name] = func
            return func

        def _register(func: Any) -> Any:
            self._commands[name] = {
                "name": name,
                "func": func,
                "doc": (getattr(func, '__doc__', None) or ''),
            }
            func.cli_arguments = _arguments
            return func

        return _register

    def _configure(self, subparsers: Any, parents: Any) -> None:
        for name in sorted(self._commands):
            self._add_subcommand(subparsers, name, parents)

    def _add_subcommand(
        self, subparsers: Any, name: str, parents: Any
    ) -> None:
        cmd = self._commands[name]
        argsfn = self._arguments.get(name)
        parser = subparsers.add_parser(
            name, parents=parents, help=cmd.get("doc", "")
        )
        if argsfn:
            argsfn(parser)
        parser.set_defaults(target=cmd['func'])


class ExpandingChoicesAction(argparse.Action):
    """Helper argparse action that lets the user just specify something
    like "samba" on the CLI but will return "CONFIG_FOR_SAMBA" like
    the gRPC enums expect, while also letting you type "CONFIG_FOR_SAMBA"
    if you are used to that.
    """

    def __init__(self, **kwargs: Any) -> None:
        # must remove choices as the parser impl. treats objects with choices
        # attr special and strictly thus breaking our transformation
        self._choices = kwargs.pop('choices')
        self._prefix = kwargs.pop('use_prefix')
        self._case_fold = kwargs.pop('case_fold', None)
        self._help = kwargs.pop('help', "Choices")
        cc = sorted(self._choices)
        ci = ''
        if self._case_fold:
            ci = ' {case insensitive}'
        kwargs['help'] = f'{self._help} (one of: {", ".join(cc)}{ci})'
        super().__init__(**kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        if real_value := self._map_choice(values):
            setattr(namespace, self.dest, real_value)
            return
        choices = ', '.join(self._choices)
        raise argparse.ArgumentError(
            self, f'invalid selection: {values} (choose from {choices})'
        )

    def _map_choice(self, value: str) -> typing.Optional[str]:
        cf = self._case_fold or str
        value = cf(value)
        if not value.startswith(cf(self._prefix)):
            value = cf(f'{self._prefix}{value}')
        choices = {cf(f'{self._prefix}{c}') for c in self._choices}
        return value if value in choices else None


class Context:
    """Command context."""

    def __init__(self, cli: Any) -> None:
        self.cli = cli

    def config(self) -> Config:
        address, chtype = self.destination()
        cc = Config(
            address=address,
            channel_type=chtype,
            tls_cert=self.cli.tls_cert,
            tls_key=self.cli.tls_key,
            tls_ca_cert=self.cli.tls_ca_cert,
            headers=self._grpc_headers(),
        )
        return cc

    def client(self) -> Client:
        return Client(self.config())

    @functools.cache
    def destination(self) -> typing.Tuple[str, ChannelType]:
        if address := getattr(self.cli, 'address', None):
            if address.startswith('unix:'):
                return address, ChannelType.INSECURE
            chtype = ChannelType.SECURE
            if getattr(self.cli, 'no_tls', False):
                chtype = ChannelType.INSECURE
            return address, chtype
        path = self._find_socket()
        return f'unix:{path}', ChannelType.INSECURE

    def _find_socket(
        self,
        envname: str = 'SMB_RCTL_SOCK_SEARCH',
        sockname: str = 'remote-control.s',
    ) -> pathlib.Path:
        search = os.environ.get(envname, DEFAULT_SOCK_SEARCH)
        if not search:
            raise DestinationError(
                'unable to find remote-control socket: no search paths'
            )
        search_roots = [pathlib.Path(p) for p in search.split(':') if p]
        matches = _find_remotectl_socket(
            sockname, search_roots, self.cli.cluster
        )
        if not matches and self.cli.cluster:
            raise DestinationError(
                f'unable to find remote-control socket: no matches in {self.cli.cluster}'
            )
        elif not matches:
            raise DestinationError(
                'unable to find remote-control socket: no matches'
            )
        elif len(matches) > 1:
            raise DestinationError(
                'unable to select remote control socket:'
                f' {len(matches)} sockets found',
                hints=['select clusters using the --cluster option'],
            )
        return matches[0]

    def _ceph_keyrings(self, searchdir: pathlib.Path) -> list[pathlib.Path]:
        try:
            keyrings = [
                p for p in searchdir.iterdir() if p.name.endswith('.keyring')
            ]
        except OSError as err:
            logger.debug("Invalid searchdir: %s", err)
            return []
        return sorted(
            keyrings,
            key=lambda p: ((0 if p.name == 'ceph.keyring' else 1), p.name),
        )

    def _grpc_headers(self) -> typing.Dict[str, str]:
        out = {}
        for key, value in self.cli.grpc_headers or []:
            out[key] = value
        address, chtype = self.destination()
        if address.startswith('unix:') and chtype is ChannelType.INSECURE:
            # probe for available ceph auth info
            krconfig = configparser.ConfigParser()
            for searchdir in [pathlib.Path('/etc/ceph')]:
                for krfile in self._ceph_keyrings(searchdir):
                    krconfig.read(krfile)
                    if krconfig.sections():
                        break
            if krconfig.sections():
                user = krconfig.sections()[0]
                key = krconfig[user]['key']
                out['ceph-auth-user'] = user
                out['ceph-auth-key'] = key
        logger.debug('gRPC headers: %r', out)
        return out

    def __str__(self) -> str:
        address, chtype = self.destination()
        hints = ''
        if any(k.startswith('ceph-auth-') for k in self._grpc_headers()):
            hints += ' with-ceph-auth'
        return f'{address} ({chtype.value}{hints})'


class ResultEncoder(json.JSONEncoder):
    """JSON Encoder that unpacks result objects that have to_simplified
    methods.
    """

    def default(self, o: Any) -> Any:
        to_simplified = getattr(o, 'to_simplified', None)
        if to_simplified:
            return to_simplified()
        return super().default(o)


def write_result_json(result: Any) -> None:
    """Write a result object as JSON to the stdout."""
    json.dump(result, sys.stdout, indent=2, cls=ResultEncoder)
    sys.stdout.write('\n')


commands = Commands()  # global commands registry


@commands.register("info")
def info(ctx: Context) -> None:
    """Get basic server info."""
    result = ctx.client().info()
    write_result_json(result)


@commands.register("status")
def status(ctx: Context) -> None:
    """Report on Samba smbd server status."""
    result = ctx.client().status()
    write_result_json(result)


@commands.register("close-share")
def close_share(ctx: Context) -> None:
    """Close a share (block IO on an existing connection)."""
    share_name = ctx.cli.share_name
    denied_users = bool(ctx.cli.denied_users_only)
    result = ctx.client().close_share(share_name, denied_users)
    write_result_json(result)


@close_share.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "share_name",
        help="Name of share to close",
    )
    parser.add_argument(
        "--denied-users-only",
        "-d",
        action="store_true",
        help="Only close the share for users that are currently denied access",
    )


@commands.register("kill-client-connection")
def kill_client_connection(ctx: Context) -> None:
    """Terminate a client connection by IP Address."""
    ip_address = ctx.cli.ip_address
    result = ctx.client().kill_client_connection(ip_address)
    write_result_json(result)


@kill_client_connection.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("ip_address", help="Address of client")


@commands.register("config-dump")
def config_dump(ctx: Context) -> None:
    """Dump configuration data."""
    source = ctx.cli.source
    hash_alg = ctx.cli.hash_alg
    result = ctx.client().config_dump(source, hash_alg=hash_alg)
    # result.dump streams output to stdout rather than buffering it
    result.dump(sys.stdout)


@config_dump.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    _cli_config_source(parser)
    _cli_config_hashes(parser)


@commands.register("config-summary")
def config_summary(ctx: Context) -> None:
    """Report a digest hash for the current configuration data."""
    source = ctx.cli.source
    hash_alg = ctx.cli.hash_alg
    result = ctx.client().config_summary(source, hash_alg=hash_alg)
    write_result_json(result)


@config_summary.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    _cli_config_source(parser)
    _cli_config_hashes(parser)


@commands.register("config-shares-list")
def config_shares_list(ctx: Context) -> None:
    """Report a list of shares configured on the server."""
    source = ctx.cli.source
    result = ctx.client().config_shares_list(source)
    write_result_json(result)


@config_shares_list.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    _cli_config_source(parser)


@commands.register("set-debug-level")
def set_debug_level(ctx: Context) -> None:
    """Set the debug level of an smb subsystem."""
    process = ctx.cli.process
    debug_level = ctx.cli.debug_level
    result = ctx.client().set_debug_level(process, debug_level)
    write_result_json(result)


@set_debug_level.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    _cli_process(parser)
    parser.add_argument("debug_level", help="Target debugging level")


@commands.register("get-debug-level")
def get_debug_level(ctx: Context) -> None:
    """Get the current debug level of an smb subsystem."""
    process = ctx.cli.process
    result = ctx.client().get_debug_level(process)
    write_result_json(result)


@get_debug_level.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    _cli_process(parser)


@commands.register("ctdb-status")
def ctdb_status(ctx: Context) -> None:
    """Report on CTDB Server status."""
    result = ctx.client().ctdb_status()
    write_result_json(result)


@commands.register("ctdb-move-ip")
def ctdb_move_ip(ctx: Context) -> None:
    """Request a CTDB public IP be moved to a different node."""
    ip_address = ctx.cli.ip_address
    node = ctx.cli.node
    result = ctx.client().ctdb_move_ip(ip_address, node)
    write_result_json(result)


@ctdb_move_ip.cli_arguments
def _(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("ip_address", help="Public IP Address to move")
    parser.add_argument("node", help="Node number of destination")


def _cli_config_source(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "source",
        choices={"samba", "ctdb", "sambacc"},
        use_prefix="config_for_",
        case_fold=str.upper,
        action=ExpandingChoicesAction,
        help="The configuration source to use",
    )


def _cli_config_hashes(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sha256",
        action="store_const",
        dest="hash_alg",
        const="HASH_ALG_SHA256",
        help="Produce a digest hash for the configuration using SHA256",
    )


def _cli_process(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "process",
        choices={"smb", "winbind", "ctdb"},
        use_prefix="smb_process_",
        case_fold=str.upper,
        action=ExpandingChoicesAction,
        help="SMB process to target",
    )


def _header(value: str) -> typing.Tuple[str, str]:
    if '=' in value:
        k, v = value.split('=', 1)
        return k, v
    if ':' in value:
        k, v = value.split(':', 1)
        return k, v
    raise argparse.ArgumentTypeError(
        'header value must take the form KEY=VALUE or KEY:VALUE'
    )


class ClusterInfo:
    def __init__(
        self, value: str, *, fsid: str = '', service: str = ''
    ) -> None:
        if '/' in value:
            fsid, service = value.split('/', 1)
        elif value:
            service = value
        if not service.startswith('smb.'):
            service = f'smb.{service}'
        self._fsid = fsid
        self._service = service

    @property
    def fsid(self) -> str:
        return self._fsid

    @property
    def service(self) -> str:
        return self._service

    def __str__(self) -> str:
        txt = f'smb service starting with {self.service}'
        if self.fsid:
            txt += f' in cluster {self.fsid}'
        return txt


def _find_remotectl_socket(
    sockname: str,
    search_roots: list[pathlib.Path],
    cluster_info: typing.Optional[ClusterInfo],
) -> list[pathlib.Path]:
    logger.debug(
        'Searching for socket paths like %s in %r (%s)',
        sockname,
        search_roots,
        cluster_info,
    )
    matches = []
    for path in search_roots:
        if path.is_socket():
            matches.append(path)
            continue
        matches.extend(p for p in path.rglob(sockname) if p.is_socket())
    logger.debug('Socket paths found: %r', matches)
    if cluster_info:
        # use cluster_info to narrow down matches
        if fsid := cluster_info.fsid:
            matches = [
                p for p in matches if any(fsid == part for part in p.parts)
            ]
        if prefix := cluster_info.service:
            matches = [
                p
                for p in matches
                if any(part.startswith(prefix) for part in p.parts)
            ]
    logger.debug('Filtered socket paths found: %r', matches)
    return matches


def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    vg = parser.add_mutually_exclusive_group()
    vg.add_argument(
        '--verbose', '-v', action='store_true', help='Enable verbose logging'
    )
    vg.add_argument(
        '--quiet', '-q', action='store_true', help='Enable minimal output'
    )
    g1 = parser.add_argument_group('Server Selection')
    mg = g1.add_mutually_exclusive_group()
    mg.add_argument(
        '--address',
        type=str,
        help='Location of gRPC server - <host_or_addr>:<port> for TCP servers',
    )
    mg.add_argument(
        '--cluster',
        '-c',
        type=ClusterInfo,
        help=(
            'Ceph SMB cluster selector.'
            ' Format [<FSID>/][smb.]<service-prefix>.'
            ' Choose the SMB service (aka SMB cluster) to connect to'
            ' using an optional Ceph FSID and SMB service.'
            ' Example: 45d9b47e-47f0-11f1-906a-525400220000/smb.cluster1'
        ),
    )
    g2 = parser.add_argument_group('TLS Credentials')
    g2.add_argument(
        '--tls-cert',
        type=TLSPath.create,
        help='Path to TLS certificate',
    )
    g2.add_argument(
        '--tls-key',
        type=TLSPath.create,
        help='Path to TLS Key',
    )
    g2.add_argument(
        '--tls-ca-cert',
        type=TLSPath.create,
        help='Path to TLS CA certificate',
    )
    parser.add_argument(
        '--no-tls',
        action='store_true',
        help='Do not use TLS when connecting to remote server',
    )
    parser.add_argument(
        '--header',
        dest='grpc_headers',
        type=_header,
        action='append',
        help='Add gRPC header to call. Takes the form <KEY>=<VALUE>',
    )
    commands._configure(parser.add_subparsers(required=True), [])
    return parser.parse_args()


def main() -> None:
    ctx = Context(parse_cli())
    log_level = logging.INFO
    if ctx.cli.verbose:
        log_level = logging.DEBUG
    elif ctx.cli.quiet:
        log_level = logging.WARNING
    logging.config.dictConfig(
        {
            "version": 1,
            "formatters": {
                "std": {"format": "%(levelname)s: %(message)s"},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "std",
                },
            },
            "root": {
                "handlers": ["console"],
                "level": log_level,
            },
        }
    )

    try:
        logger.info('Connecting to remote-control server at %s', str(ctx))
        ctx.cli.target(ctx)
    except DestinationError as err:
        print(f'error: {err}', file=sys.stderr)
        for hint in err.hints or []:
            print(f'       ({hint})', file=sys.stderr)
        sys.exit(2)
    except APICallError as err:
        print('error: failed gRPC call:', file=sys.stderr)
        print(f'  code = {err.code}', file=sys.stderr)
        print(f'  details = {err.details}', file=sys.stderr)
        print(f'  result = {err.msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
