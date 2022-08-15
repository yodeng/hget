import os
import sys
import time
import signal
import base64
import logging
import socket
import struct
import asyncio
import aioftp
import argparse
import functools
import subprocess

from copy import deepcopy
from threading import Thread
from multiprocessing import cpu_count

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector, ClientTimeout
from aiohttp.client_reqrep import ClientRequest
from aiohttp.client_exceptions import *

from ._version import __version__


class exitSync(Thread):
    def __init__(self, obj=None, daemon=True):
        super(exitSync, self).__init__(daemon=daemon)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.obj = obj

    def run(self):
        time.sleep(1)

    def signal_handler(self, signum, frame):
        self.obj.write_offset()
        self.obj.loger.debug("Update %s before exit",
                             os.path.basename(self.obj.rang_file))
        sys.exit(signum)


class KeepAliveClientRequest(ClientRequest):
    async def send(self, conn: "Connection") -> "ClientResponse":
        sock = conn.protocol.transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        return (await super().send(conn))


default_headers = {
    "Connection": "close",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
}

ReloadException = (
    ServerDisconnectedError,
    OSError,
    ClientPayloadError,
)


def max_async():
    max_as = 50
    n_cpu = cpu_count()
    if n_cpu > 60:
        max_as = Chunk.MAX_AS
    elif n_cpu > 30:
        max_as = 200
    elif n_cpu > 20:
        max_as = 100
    elif n_cpu > 10:
        max_as = 50
    else:
        max_as = 20
    return max(max_as, 1)


def get_as_part(filesize):
    mn_as, mn_pt = 10, 20
    min_as, min_pt = 10, 10
    if filesize > 1 * Chunk.OneG:
        mn_pt = Chunk.MAX_PT
    elif filesize > 500 * Chunk.OneM:
        mn_pt = filesize // (Chunk.OneM*1.3)
    elif filesize > 100 * Chunk.OneM:
        mn_pt = filesize // (Chunk.OneM * 1.2)
    else:
        mn_pt = filesize // (Chunk.OneM * 2)
    mn_pt = min(mn_pt, Chunk.MAX_PT)
    mn_as = max_async()
    mn_as = min(mn_as, mn_pt)
    return max(int(mn_as), min_as), max(int(mn_pt), min_pt)


class Chunk(object):
    OneK = 1024
    OneM = OneK * OneK
    OneG = OneM * OneK
    OneT = OneG * OneK
    MAX_AS = 300
    MAX_PT = 1000


class TimeoutException(Exception):
    pass


def human_size(num):
    for unit in ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s" % (num, unit)
        num /= 1024.0
    return "%.1f%s" % (num, 'Y')


def loger(logfile=None, level="info"):
    logger = logging.getLogger()
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
        f = logging.Formatter(
            '[%(levelname)s %(asctime)s] %(message)s')
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
        f = logging.Formatter(
            '[%(levelname)s %(threadName)s %(asctime)s %(funcName)s(%(lineno)d)] %(message)s')
    if logfile is None:
        h = logging.StreamHandler(sys.stdout)
    else:
        h = logging.FileHandler(logfile, mode='w')
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def mkdir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def mkfile(filename, size):
    with open(filename, 'wb') as f:
        f.seek(size-1)
        f.write(b'\x00')


def parseArg():
    parser = argparse.ArgumentParser(
        description="An interruptable and resumable download accelerator supplementary of wget/axel.")
    parser.add_argument("-o", "--output", type=str,
                        help='output download file', metavar="<file>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max number of async concurrency (not thread or process), default: auto", metavar="<int>")
    parser.add_argument("-c", "--connections", type=int,
                        help="the max number of tcp connections for http/https. more tcp connections can speedup, but might be forbidden by url server, default: auto", metavar="<int>")
    parser.add_argument('-t', '--timeout', type=int, default=30,
                        help='timeout for download, 30s by default', metavar="<int>")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='logging debug', default=False)
    parser.add_argument('-q', '--quite', action='store_true',
                        help='suppress all output except error or download success', default=False)
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    parser.add_argument('--noreload', dest='use_reloader', action='store_false',
                        help='tells hget to NOT use the auto-reloader')
    parser.add_argument("url", type=str,
                        help="download url, http/https/ftp support", metavar="<url>")
    return parser.parse_args()


def restart_with_reloader():
    new_environ = os.environ.copy()
    while True:
        args = [sys.executable] + sys.argv
        new_environ["RUN_MAIN"] = 'true'
        exit_code = subprocess.call(args, env=new_environ)
        if exit_code != 3:
            return exit_code
        new_environ["RUN_HGET_FIRST"] = "false"
        time.sleep(0.1)


def autoreloader(main_func, *args, **kwargs):
    if os.environ.get("RUN_MAIN") == "true":
        main_func(*args, **kwargs)
    else:
        try:
            exit_code = restart_with_reloader()
            if exit_code < 0:
                os.kill(os.getpid(), -exit_code)
            else:
                sys.exit(exit_code)
        except KeyboardInterrupt:
            pass
