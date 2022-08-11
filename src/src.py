#!/usr/bin/env python

import os
import sys

from .utils import *


class Download(object):
    def __init__(self, url="", outfile="", threads=Chunk.MAX_AS, headers={}, quite=False, tcp_conn=None, **kwargs):
        self.url = url
        self.outfile = os.path.abspath(outfile)
        self.outdir = os.path.dirname(self.outfile)
        self.rang_file = self.outfile + ".ht"
        mkdir(self.outdir)
        self.threads = threads or min(max_async(), threads or Chunk.MAX_AS)
        self.parts = Chunk.MAX_PT
        self.tcp_conn = tcp_conn
        self.timeout = ClientTimeout(total=60*60*24, sock_read=2400)
        self.connector = TCPConnector(
            limit=self.tcp_conn, verify_ssl=False)
        self.headers = headers
        self.headers.update(default_headers)
        self.offset = {}
        self.range_list = []
        self.tqdm_init = 0
        self.set_sem(self.threads)
        self.Retry = Retry
        self.quite = quite
        self.extra = kwargs

    def get_range(self, content_length, size=1024 * 1000):
        if os.path.isfile(self.rang_file):
            lines = self.load_offset()
            for line in lines:
                if not line.strip():
                    continue
                end, start0, start, read = map(int, line.strip().split())
                self.tqdm_init += start - start0 + 1 + read
                if read > 0:
                    start = read - 1 + start
                    read = 0
                    self.tqdm_init -= 1
                rang = {'Range': (start, end)}
                self.offset[end] = [start0, start, read]
                self.range_list.append(rang)
                self.content_length = end
            self.loger.debug("%s of %s has download" %
                             (self.tqdm_init, self.content_length))
        else:
            self.content_length = content_length
            if self.parts:
                size = content_length // self.parts
                count = self.parts
            else:
                count = content_length // size
            for i in range(count):
                start = i * size
                if i == count - 1:
                    end = content_length
                else:
                    end = start + size
                if i > 0:
                    start += 1
                rang = {'Range': (start, end)}
                self.offset[end] = [start, start, 0]
                self.range_list.append(rang)
            if not os.path.isfile(self.outfile):
                mkfile(self.outfile, self.content_length)
            self.write_offset()

    async def download(self):
        async with ClientSession(connector=self.connector, timeout=self.timeout) as session:
            req = await self.fetch(session)
            content_length = int(req.headers['content-length'])
            mn_as, mn_pt = get_as_part(content_length)
            self.threads = min(self.threads, mn_as)
            self.parts = min(self.parts, mn_pt)
            self.set_sem(self.threads)
            self.get_range(content_length)
            if len(self.Retry) == 0:
                self.loger.info("File size: %s (%d bytes)",
                                human_size(content_length), content_length)
                self.loger.info("Starting download %s --> %s",
                                self.url, self.outfile)
                self.Retry.append(content_length)
            self.loger.info("Ranges: %s, Sem: %s, Connections: %s, %s", self.parts,
                            self.threads, self.tcp_conn or 100, get_as_part(content_length))
            with tqdm(disable=self.quite, total=int(self.content_length), initial=self.tqdm_init, unit='', ascii=True, unit_scale=True) as bar:
                tasks = []
                for h_range in self.range_list:
                    s, e = h_range["Range"]
                    if s == e:
                        continue
                    h_range.update(self.headers)
                    task = self.fetch(
                        session, pbar=bar, headers=h_range.copy())
                    tasks.append(task)
                await asyncio.gather(*tasks)

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
                                self.offset[e][-1] += len(chunk)
                                pbar.update(len(chunk))
                self.loger.debug(
                    "Finished %s %s", asyncio.current_task().get_name(), headers["Range"])
        else:
            async with session.get(self.url) as req:
                return req

    def set_sem(self, n):
        self.sem = asyncio.Semaphore(n)

    def run(self):
        if not self.url.startswith("http"):
            self.loger.error("Only http or https urls allowed")
            sys.exit(1)
        Done = False
        try:
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.download())
            Done = True
        except Exception as e:
            self.loger.debug(e)
            # raise e
        finally:
            self.write_offset()
        return Done

    def write_offset(self):
        if len(self.offset):
            with open(self.rang_file, "wb") as fo:
                fo.write(struct.pack('<Q', len(self.offset)))
                fo.write(os.urandom(3))
                for e, s in self.offset.items():
                    l = [e, ] + s
                    for i in l:
                        fo.write(struct.pack('<Q', int(i)))
                        fo.write(os.urandom(3))

    def load_offset(self):
        out = []
        with open(self.rang_file, "rb") as fi:
            fileno = struct.unpack('<Q', fi.read(8))[0]
            fi.read(3)
            for i in range(fileno):
                offset = []
                for j in range(4):
                    p = struct.unpack('<Q', fi.read(8))[0]
                    offset.append(p)
                    fi.read(3)
                out.append("\t".join(map(str, offset)))
        return out

    @property
    def loger(self):
        log = logging.getLogger()
        if self.quite:
            log.setLevel(logging.ERROR)
        return log


@retry(wait=wait_fixed(1), retry=retry_if_result(lambda v: not v) | retry_if_exception_type())
def hget(url="", outfile="", threads=Chunk.MAX_AS, quite=False, tcp_conn=None, **kwargs):
    dn = Download(url=url, outfile=outfile,
                  threads=threads, quite=quite, tcp_conn=tcp_conn, **kwargs)
    es = exitSync(obj=dn)
    es.start()
    res = dn.run()
    if res:
        if os.path.isfile(dn.rang_file):
            os.remove(dn.rang_file)
            dn.loger.debug("Remove %s", os.path.basename(dn.rang_file))
    return res
