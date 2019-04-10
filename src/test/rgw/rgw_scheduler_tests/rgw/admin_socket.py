import asyncio
import json
import socket
import struct
import logging

async def async_perf_dump(sock_path):
    perf_dump = await admin_sock_cmd(sock_path, {"prefix": "perf dump"})
    return perf_dump['simple-throttler']

async def admin_sock_cmd(sock_path, cmd_dict):
    # TODO: can we ctx mgr here?
    reader, writer = await asyncio.open_unix_connection(path=sock_path)
    cmd_str = json.dumps(cmd_dict).encode() + b'\0'
    writer.write(cmd_str)
    await writer.drain()

    data_len_b = await reader.read(n=4)
    data_len = int.from_bytes(data_len_b, byteorder='big')

    data_b = await reader.read(data_len)
    data_json = json.loads(data_b)
    writer.close()

    return data_json
