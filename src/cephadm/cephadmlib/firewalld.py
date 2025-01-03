# firewalld.py - functions and types for working with firewalld

import logging

from typing import List, Dict

from .call_wrappers import call, call_throws, CallVerbosity
from .context import CephadmContext
from .daemon_form import DaemonForm, FirewalledServiceDaemonForm
from .exe_utils import find_executable
from .systemd import check_unit

logger = logging.getLogger()


class Firewalld(object):
    # for specifying ports we should always open when opening
    # ports for a daemon of that type. Main use case is for ports
    # that we should open when deploying the daemon type but that
    # the daemon itself may not necessarily need to bind to the port.
    # This needs to be handed differently as we don't want to fail
    # deployment if the port cannot be bound to but we still want to
    # open the port in the firewall.
    external_ports: Dict[str, List[int]] = {
        'iscsi': [3260]  # 3260 is the well known iSCSI port
    }

    def __init__(self, ctx):
        # type: (CephadmContext) -> None
        self.ctx = ctx
        self.available = self.check()

    def check(self):
        # type: () -> bool
        self.cmd = find_executable('firewall-cmd')
        if not self.cmd:
            logger.debug('firewalld does not appear to be present')
            return False
        (enabled, state, _) = check_unit(self.ctx, 'firewalld.service')
        if not enabled:
            logger.debug('firewalld.service is not enabled')
            return False
        if state != 'running':
            logger.debug('firewalld.service is not running')
            return False

        logger.info('firewalld ready')
        return True

    def enable_service_for(self, svc: str) -> None:
        assert svc, 'service name not provided'
        if not self.available:
            logger.debug(
                'Not possible to enable service <%s>. firewalld.service is not available'
                % svc
            )
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        out, err, ret = call(
            self.ctx,
            [self.cmd, '--permanent', '--query-service', svc],
            verbosity=CallVerbosity.DEBUG,
        )
        if ret:
            logger.info(
                'Enabling firewalld service %s in current zone...' % svc
            )
            out, err, ret = call(
                self.ctx, [self.cmd, '--permanent', '--add-service', svc]
            )
            if ret:
                raise RuntimeError(
                    'unable to add service %s to current zone: %s'
                    % (svc, err)
                )
        else:
            logger.debug(
                'firewalld service %s is enabled in current zone' % svc
            )

    def open_ports(self, fw_ports):
        # type: (List[int]) -> None
        if not self.available:
            logger.debug(
                'Not possible to open ports <%s>. firewalld.service is not available'
                % fw_ports
            )
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        for port in fw_ports:
            tcp_port = str(port) + '/tcp'
            out, err, ret = call(
                self.ctx,
                [self.cmd, '--permanent', '--query-port', tcp_port],
                verbosity=CallVerbosity.DEBUG,
            )
            if ret:
                logger.info(
                    'Enabling firewalld port %s in current zone...' % tcp_port
                )
                out, err, ret = call(
                    self.ctx,
                    [self.cmd, '--permanent', '--add-port', tcp_port],
                )
                if ret:
                    raise RuntimeError(
                        'unable to add port %s to current zone: %s'
                        % (tcp_port, err)
                    )
            else:
                logger.debug(
                    'firewalld port %s is enabled in current zone' % tcp_port
                )

    def close_ports(self, fw_ports):
        # type: (List[int]) -> None
        if not self.available:
            logger.debug(
                'Not possible to close ports <%s>. firewalld.service is not available'
                % fw_ports
            )
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        for port in fw_ports:
            tcp_port = str(port) + '/tcp'
            out, err, ret = call(
                self.ctx,
                [self.cmd, '--permanent', '--query-port', tcp_port],
                verbosity=CallVerbosity.DEBUG,
            )
            if not ret:
                logger.info('Disabling port %s in current zone...' % tcp_port)
                out, err, ret = call(
                    self.ctx,
                    [self.cmd, '--permanent', '--remove-port', tcp_port],
                )
                if ret:
                    raise RuntimeError(
                        'unable to remove port %s from current zone: %s'
                        % (tcp_port, err)
                    )
                else:
                    logger.info(f'Port {tcp_port} disabled')
            else:
                logger.info(f'firewalld port {tcp_port} already closed')

    def apply_rules(self):
        # type: () -> None
        if not self.available:
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        call_throws(self.ctx, [self.cmd, '--reload'])


def update_firewalld(ctx: CephadmContext, daemon: DaemonForm) -> None:
    if not ('skip_firewalld' in ctx and ctx.skip_firewalld) and isinstance(
        daemon, FirewalledServiceDaemonForm
    ):
        svc = daemon.firewall_service_name()
        if not svc:
            return
        firewall = Firewalld(ctx)
        firewall.enable_service_for(svc)
        firewall.apply_rules()
