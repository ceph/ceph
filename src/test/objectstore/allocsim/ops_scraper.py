#!/bin/python3.11
import argparse
import hashlib
import json
import subprocess
from typing import List, Dict, Any, Optional
import datetime
import sys
import time
import logging

FORMAT = '%(name)8s::%(levelname)8s:: %(message)s'
HISTORY_HOLD = 2
HISTORY_SAFE_RATE = 1.5
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('osd-op-scrapper')


class Env:
    _args = None
    _store = {}

    @classmethod
    def args(cls):
        return cls._args

    @classmethod
    def store(cls):
        return cls._store

    @classmethod
    def setup_env(cls, args):
        if cls._args is not None:
            logger.error('double setup')
            sys.exit(1)
        cls._args = args


class OpDescription:
    def __init__(self, reqid, pg, object_name, operations: List[List[str]], initiated_at):
        self.reqid = reqid
        self.pg = pg
        self.object_name = object_name
        self.operations = operations
        self.initiated_at = initiated_at

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        res = ''
        i = 0
        while i < len(self.operations):
            op_type = self.operations[i][0]
            if op_type in ['write', 'writefull', 'read', 
                                      'sync_read', 'sparse-read', 
                                      'zero', 'append', 'mapext', 
                                      'cmpext']:
                if len(self.operations[i]) < 2:
                    logger.error('malformed input, expected extents not found')
                    logger.error(self.operations)
                    break
                extents = self.operations[i][1]
                res += f'{self.initiated_at} {self.reqid} {op_type} {extents} {self.object_name} {self.pg}'
            elif op_type in ['truncate']:
                offset = self.operations[i][1]
                if len(self.operations) < 2:
                    logger.error('malformed input, expected offset not found')
                    break
                res += f'{self.initiated_at} {self.reqid} {op_type} {offset} {self.object_name} {self.pg}'

            i += 1

        return res


def run_ceph_command(command: List[str], no_out=False) -> Any:
    command.insert(0, 'ceph')
    if Env.args().ceph_bin_path:
        command[0] = Env.args().ceph_bin_path

    command.append('--format')
    command.append('json')

    res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode != 0:
        logger.error(f'error executing "{command}": \n{res.stderr}\n')
    if no_out:
        return None
    return json.loads(res.stdout)

def osd_ls() -> List[str]:
    return run_ceph_command(['osd', 'ls'])

def parse_osd_op(description_literal: str, initiated_at: datetime.datetime):
    prev = 0
    cursor = description_literal.find(' ', 0)
    reqid = description_literal[prev:cursor]

    prev = cursor
    cursor = description_literal.find(' ', cursor+1)
    pgid = description_literal[prev+1:cursor]

    prev = cursor
    cursor = description_literal.find(' ', cursor+1)
    object_name = description_literal[prev+1:cursor]
    object_name_split = object_name.split(':')
    if not Env.args().nohash :
        if len(object_name_split) < 2:
            object_name = hashlib.md5(object_name.encode()).hexdigest()
        else:
            object_name_split[-2] = hashlib.md5(object_name_split[-2].encode()).hexdigest()
            object_name = ':'.join(object_name_split)

    prev = cursor
    cursor = description_literal.find(']', cursor+1)
    operations = description_literal[prev+2:cursor].split(',')
    for i in range(len(operations)):
        operations[i] = operations[i].split(' ')

    return OpDescription(reqid, pgid, object_name, operations, initiated_at)

class ProcessInfo:
    def __init__(self, process_time: int, command_time: int, processed_info: str, ops_count: int,
                 new_ops: int, oldest_operation_at: Optional[datetime.date],
                 capture_period_start: int, capture_period_end: int):
        self.process_time = process_time
        self.command_time = command_time
        self.processed_info = processed_info
        self.ops_count = ops_count
        self.new_ops = new_ops
        self.oldest_operation_at = oldest_operation_at
        self.capture_period_start = capture_period_start
        self.capture_period_end = capture_period_end

def _to_timestamp(date: datetime.datetime) -> int:
    return date.astimezone().timestamp()

def process_osd(osd_id) -> ProcessInfo:
    if osd_id not in Env.store():
        Env.store()[osd_id] = {}
        Env.store()[osd_id]['last_op'] = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)


    logger.info(f'collecting from osd.{osd_id}')
    command_time_start = time.time()
    historic: Dict[str, Any] = run_ceph_command(['tell', f'osd.{osd_id}', 'dump_historic_ops'])
    command_time = time.time() - command_time_start

    osd_processing_start = time.time()

    operations_str = 'initiated_at | who | op_type | offset/extent | name | pgid\n'
    operations_str += '-------------------------------------------------------\n'
    new_ops = 0 
    oldest_operation_at = None
    capture_period_start = _to_timestamp(Env.store()[osd_id]['last_op'])
    for op in historic['ops']:
        if not oldest_operation_at:
            #we twice parse initated_at, but code is simpler that way
            initiated_at = datetime.datetime.fromisoformat(op['initiated_at'])
            oldest_operation_at = initiated_at
        description = op['description']
        logger.debug(f'{description}')
        if not description.startswith('osd_op('):
            continue
        initiated_at = datetime.datetime.fromisoformat(op['initiated_at'])
        if initiated_at < Env.store()[osd_id]['last_op']:
            continue
        new_ops += 1
        Env.store()[osd_id]['last_op'] = initiated_at
        description_data = description[7:-1]
        op_data = parse_osd_op(description_data, initiated_at)
        operations_str += f'{str(op_data)}\n'
    capture_period_end = time.time()
    processing_time = time.time() - osd_processing_start
    logger.info(f'osd.{osd_id} new_ops {new_ops}')

    return ProcessInfo(processing_time, command_time, operations_str, len(historic['ops']), 
                       new_ops, oldest_operation_at, capture_period_start, capture_period_end)

def _set_osd_history_size(name: str, history_size: int):
    run_ceph_command(['tell', f'osd.{name}', 'config', 'set', 'osd_op_history_size',
                      str(int(history_size))], no_out=True)

def _set_osd_history_duration(name: str, duration: int):
    run_ceph_command(['tell', f'osd.{name}', 'config', 'set', 'osd_op_history_duration',
                      str(duration)], no_out=True)

class OsdParameters:
    def __init__(self, name: str, ready_time: int, freq: int, history_size: int) -> None:
        self.name = name 
        self.ready_time = ready_time 
        self.freq = freq
        self.history_size = history_size
        self.sum_ops = 0

def main():
    parser = argparse.ArgumentParser(
            prog='OSD operations parser')
    parser.add_argument('--nohash',
                        action='store_true', required=False)
    parser.add_argument('--debug_level', type=str, default='1')
    parser.add_argument('--ceph_bin_path', required=False)
    parser.add_argument('--freq', required=False, type=int, default=1)
    parser.add_argument('--min_history_size', required=False, type=int, default=100)
    parser.add_argument('--max_history_size', required=False, type=int, default=1000)
    parser.add_argument('--osds', required=True, type=str, default='0,1,2', 
                        help='Comma separated list of osd names to parse. Default: "0,1,2"')
    parser.add_argument('--out', required=False, help="filename to write output to. If none is provided it will be written to stdout")
    args = parser.parse_args()

    Env.setup_env(args)

    log_levels = {
            '1': logging.CRITICAL,
            '2': logging.ERROR,
            '3': logging.WARNING,
            '4': logging.INFO,
            '5': logging.DEBUG,
            '6': logging.NOTSET
    }

    logger.setLevel(log_levels[Env.args().debug_level.upper()])
    logger.debug(str(Env.args()))
    logger.debug(str(osd_ls()))

    outfile = sys.stdout

    if Env.args().out:
        outfile = open(Env.args().out, 'w+')

    pref_freq = int(Env.args().freq)
    freq = int(Env.args().freq)
    freq = 1.5
    sleep_time = 0
    sum_time_elapsed = 0
    min_history_size = int(Env.args().min_history_size)
    max_history_size = int(Env.args().max_history_size)
    history_size = min_history_size
    # -------------|-------------------|
    history_overlap = 1.10
    osds = Env.args().osds.split(',')
    osds_info = []
    for osd in osds:
        _set_osd_history_size(osd, history_size)
        _set_osd_history_duration(osd, HISTORY_HOLD) #fixed 2s duration to hold ops
        osds_info.append(OsdParameters(osd, 0, freq, history_size))

    logger.debug(f'start freq {freq}')
    while True:
        logger.debug(f'sleep sec {sleep_time}')
        time.sleep(sleep_time)
        sleep_time = freq
        for osd in osds_info:

            if osd.ready_time >= time.time():
                continue
            process_info = process_osd(osd.name)

            #basically, we need to have so much history, that it will never be full
            #full history means that we lost something, its that easy
            if osd.history_size < process_info.ops_count * HISTORY_SAFE_RATE:
                want_history_size = min(int(process_info.ops_count * HISTORY_SAFE_RATE), max_history_size)
                logger.info(f'changing osd.{osd.name} history size from {osd.history_size} to {want_history_size}')
                _set_osd_history_size(osd.name, want_history_size)
                osd.history_size = want_history_size

            if not process_info.new_ops:
                new_ops_period = 1
            else:
                oldest_initiated_at = process_info.oldest_operation_at
                new_ops_period = process_info.capture_period_end - _to_timestamp(oldest_initiated_at)
            capture_period = process_info.capture_period_end - process_info.capture_period_start

            sum_time_elapsed += capture_period
            osd.sum_ops += process_info.new_ops

            if process_info.new_ops >= max_history_size:
                lost_ops = ((process_info.new_ops * capture_period) / (new_ops_period)) - process_info.new_ops
                logger.debug(f'process_info.new_ops: {process_info.new_ops} capture_period: {capture_period} new_ops_period: {new_ops_period} start: {process_info.capture_period_start} end: {process_info.capture_period_end}')
                osd.sum_ops += lost_ops




            outfile.write(process_info.processed_info)
            outfile.flush()

            sleep_time -= process_info.process_time + process_info.command_time
            now = time.time()
            osd.ready_time = now + (osd.freq - process_info.process_time - process_info.command_time)
            sleep_time = max(0, sleep_time)
            logger.info(f'osd.{osd.name} parsing dump_historic_ops with {process_info.ops_count} ops took {process_info.process_time}')
            logger.info(f'osd.{osd.name} command dump_historic_ops with {process_info.ops_count} ops took {process_info.command_time}')

    close(outfile)

if __name__ == '__main__':
    main()
