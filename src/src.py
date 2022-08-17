#!/usr/bin/env python

import os
import sys
import _thread

from .utils import *


class Download(object):
    def __init__(self, url="", outfile="", threads=Chunk.MAX_AS, headers={}, quite=False, tcp_conn=None, timeout=30, **kwargs):
        self.url = url
        if outfile:
            self.outfile = os.path.abspath(outfile)
        else:
            self.outfile = os.path.join(
                os.getcwd(), os.path.basename(self.url))
        self.outdir = os.path.dirname(self.outfile)
        self.rang_file = self.outfile + ".ht"
        mkdir(self.outdir)
        self.threads = threads
        self.parts = Chunk.MAX_PT
        self.tcp_conn = tcp_conn
        self.datatimeout = timeout
        self.headers = headers
        self.headers.update(default_headers)
        self.offset = {}
        self.range_list = []
        self.tqdm_init = 0
        self.quite = quite
        self.extra = kwargs
        self.ftp = False
        self.startime = int(time.time())

    async def get_range(self, session, size=1024 * 1000):
        if os.path.isfile(self.rang_file) and os.path.isfile(self.outfile):
            self.load_offset()
            content_length = 0
            for end, s in self.offset.items():
                start0, read = s
                self.tqdm_init += read
                start = sum(s)
                rang = {'Range': (start, end)}
                self.range_list.append(rang)
                content_length = max(content_length, end + 1)
            mn_as, mn_pt = get_as_part(content_length)
            self.threads = self.threads or min(
                max_async(), Chunk.MAX_AS, mn_as)
            self.parts = min(self.parts, mn_pt)
            self.content_length = content_length
            self.loger.debug("%s of %s has download" %
                             (self.tqdm_init, self.content_length))
        else:
            req = await self.fetch(session)
            content_length = int(req.headers['content-length'])
            mn_as, mn_pt = get_as_part(content_length)
            self.threads = self.threads or min(
                max_async(), Chunk.MAX_AS, mn_as)
            self.parts = min(self.parts, mn_pt)
            self.content_length = content_length
            if self.parts:
                size = content_length // self.parts
                count = self.parts
            else:
                count = content_length // size
            for i in range(count):
                start = i * size
                if i == count - 1:
                    end = content_length - 1
                else:
                    end = start + size
                if i > 0:
                    start += 1
                rang = {'Range': (start, end)}
                self.offset[end] = [start, 0]
                self.range_list.append(rang)
            if not os.path.isfile(self.outfile):
                mkfile(self.outfile, self.content_length)
            self.write_offset()

    async def download(self):
        self.timeout = ClientTimeout(total=60*60*24, sock_read=2400)
        self.connector = TCPConnector(
            limit=self.tcp_conn, verify_ssl=False)
        async with ClientSession(connector=self.connector, timeout=self.timeout) as session:
            await self.get_range(session)
            self.set_sem(self.threads)
            if os.getenv("RUN_HGET_FIRST") != 'false':
                self.loger.info("File size: %s (%d bytes)",
                                human_size(self.content_length), self.content_length)
                self.loger.info("Starting download %s --> %s",
                                self.url, self.outfile)
            self.loger.debug("Ranges: %s, Sem: %s, Connections: %s, %s", self.parts,
                             self.threads, self.tcp_conn or 100, get_as_part(self.content_length))
            with tqdm(disable=self.quite, total=int(self.content_length), initial=self.tqdm_init, unit='', ascii=True, unit_scale=True) as bar:
                tasks = []
                for h_range in self.range_list:
                    s, e = h_range["Range"]
                    if s > e:
                        continue
                    h_range.update(self.headers)
                    task = self.fetch(
                        session, pbar=bar, headers=h_range.copy())
                    tasks.append(task)
                await asyncio.gather(*tasks)

    async def download_ftp(self):
        host, filepath = self.url.split(":")[-1].lstrip("/").split("/", 1)
        if host and filepath:
            size = 0
            if os.path.isfile(self.outfile):
                size = os.path.getsize(self.outfile)
            async with aioftp.Client.context(host) as client:
                if await client.is_file(filepath):
                    stat = await client.stat(filepath)
                    self.content_length = int(stat["size"])
                    if os.getenv("RUN_HGET_FIRST") != 'false':
                        self.loger.info("Logging in as anonymous success")
                        self.loger.info("File size: %s (%d bytes)",
                                        human_size(self.content_length), self.content_length)
                        self.loger.info("Starting download %s --> %s",
                                        self.url, self.outfile)
                    with tqdm(disable=self.quite, total=int(self.content_length), initial=size, unit='', ascii=True, unit_scale=True) as bar:
                        self.loger.debug(
                            "Start %s %s", asyncio.current_task().get_name(), 'bytes={0}-{1}'.format(size, self.content_length))
                        async with client.download_stream(filepath, offset=size) as stream:
                            with open(self.outfile, size and "ab" or "wb") as f:
                                f.seek(size, os.SEEK_SET)
                                async for chunk in stream.iter_by_block(1024):
                                    if chunk:
                                        f.write(chunk)
                                        f.flush()
                                        bar.update(len(chunk))
                        self.loger.debug(
                            "Finished %s %s", asyncio.current_task().get_name(), 'bytes={0}-{1}'.format(size, self.content_length))

    async def fetch(self, session, pbar=None, headers=None):
        if headers:
            async with self.sem:
                s, e = headers["Range"]
                headers["Range"] = 'bytes={0}-{1}'.format(s, e)
                self.loger.debug(
                    "Start %s %s", asyncio.current_task().get_name(), headers["Range"])
                async with session.get(self.url, headers=headers, timeout=self.timeout, params=self.extra) as req:
                    with open(self.outfile, 'r+b') as f:
                        f.seek(s, os.SEEK_SET)
                        async for chunk in req.content.iter_chunked(102400):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                                self.offset[e][-1] += len(chunk)
                                pbar.update(len(chunk))
                self.loger.debug(
                    "Finished %s %s", asyncio.current_task().get_name(), headers["Range"])
        else:
            async with session.get(self.url, timeout=self.datatimeout) as req:
                return req

    def set_sem(self, n):
        self.sem = asyncio.Semaphore(n)

    def run(self):
        self.startime = int(time.time())
        _thread.start_new_thread(self.check_offset, (self.datatimeout,))
        Done = False
        try:
            if self.url.startswith("http"):
                self.loop = asyncio.get_event_loop()
                self.loop.run_until_complete(self.download())
            elif self.url.startswith("ftp"):
                self.ftp = True
                asyncio.run(self.download_ftp())
            else:
                self.loger.error("Only http/https or ftp urls allowed.")
                sys.exit(1)
            Done = True
        except ReloadException as e:
            self.loger.debug(e)
            if os.environ.get("RUN_MAIN") == "true":
                sys.exit(3)
            raise e
        except asyncio.TimeoutError as e:
            raise TimeoutException("Connect url timeout")
        except Exception as e:
            self.loger.debug(e)
            raise e
        finally:
            self.write_offset()
        return Done

    def write_offset(self):
        if len(self.offset):
            with open(self.rang_file, "wb") as fo:
                fo.write(struct.pack('<Q', self.startime))
                fo.write(struct.pack('<Q', len(self.offset)))
                fo.write(os.urandom(3))
                for e, s in self.offset.items():
                    l = [e, ] + s
                    for i in l:
                        fo.write(struct.pack('<Q', int(i)))
                        fo.write(os.urandom(3))

    def load_offset(self):
        with open(self.rang_file, "rb") as fi:
            self.startime = struct.unpack('<Q', fi.read(8))[0]
            fileno = struct.unpack('<Q', fi.read(8))[0]
            fi.read(3)
            for i in range(fileno):
                offset = []
                for j in range(3):
                    p = struct.unpack('<Q', fi.read(8))[0]
                    offset.append(p)
                    fi.read(3)
                self.offset[offset[0]] = offset[1:]

    @property
    def loger(self):
        log = logging.getLogger()
        if self.quite:
            log.setLevel(logging.ERROR)
        return log

    def check_offset(self, timeout=30):
        time.sleep(5)
        while True:
            o = self._get_data
            time.sleep(timeout)
            if o == self._get_data:
                if (self.ftp and o == self.content_length):
                    return
                elif len(self.offset):
                    for k, v in self.offset.items():
                        if sum(v) != k+1:
                            break
                    else:
                        return
                self._exit_without_data(timeout)
            else:
                self.loger.debug("data is downloading")

    @property
    def _get_data(self):
        data = 0
        if self.ftp:
            if os.path.isfile(self.outfile):
                data = os.path.getsize(self.outfile)
        else:
            data = deepcopy(self.offset)
        return data

    def _exit_without_data(self, timeout):
        if os.environ.get("RUN_MAIN") == "true":
            self.loger.debug(
                "Any data gets in %s sec, Exit 3", timeout)
        else:
            self.loger.error(
                "Any data gets in %s sec, Exit 3", timeout)
        self.write_offset()
        os._exit(3)


def hget(url="", outfile="", threads=Chunk.MAX_AS, quite=False, tcp_conn=None, timeout=30, **kwargs):
    dn = Download(url=url, outfile=outfile,
                  threads=threads, quite=quite, tcp_conn=tcp_conn, timeout=timeout,  **kwargs)
    es = exitSync(obj=dn)
    es.start()
    res = dn.run()
    if res:
        if os.path.isfile(dn.rang_file):
            os.remove(dn.rang_file)
            dn.loger.debug("Remove %s", os.path.basename(dn.rang_file))
        els = int(time.time()) - dn.startime
        dn.loger.info("Donwload success, time elapse: %s sec", els)
    return res
