#!/usr/bin/env python

import os
import sys
import _thread

from .utils import *
from .ftp import login


class Download(object):

    def __init__(self, url="", outfile="", threads=Chunk.MAX_AS, headers={}, quiet=False, tcp_conn=None, timeout=30, **kwargs):
        self.url = url
        if outfile:
            self.outfile = os.path.abspath(outfile)
        else:
            self.outfile = os.path.join(
                os.getcwd(), os.path.basename(urlparse(self.url).path))
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
        self.quiet = quiet
        self.extra = remove_empty_items(kwargs)
        self.max_speed = hs_decode(self.extra.get(
            "max_speed") or sys.maxsize)
        self.chunk_size = 100 * Chunk.OneK
        self.ftp = False
        self.startime = int(time.time())
        self.current = current_process()
        self.rate_limiter = RateLimit(
            max(int(float(self.max_speed)/self.chunk_size), 1))
        self.lock = Lock()

    async def get_range(self, session=None, size=1000*Chunk.OneK):
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
            if not isinstance(req, int):
                if 400 <= req.status < 500:
                    raise DownloadError(
                        "Client ERROR %s: %s bad request" % (req.status, self.url))
                elif 500 <= req.status:
                    raise DownloadError(
                        "Server ERROR %s: %s bad request" % (req.status, self.url))
                content_length = int(req.headers['content-length'])
            else:
                content_length = req
            self.content_length = content_length
            if self.content_length < 1:
                self.offset = {}
                self.loger.error(
                    "Remote file has no content with filesize 0 bytes.")
                sys.exit(1)
            if hasattr(req, "headers") and "accept-ranges" not in req.headers:
                rang = {'Range': (0, content_length-1)}
                self.offset[content_length-1] = [0, 0]
                self.range_list.append(rang)
                self.threads = self.parts = 1
            else:
                mn_as, mn_pt = get_as_part(content_length)
                self.threads = self.threads or min(
                    max_async(), Chunk.MAX_AS, mn_as)
                self.parts = min(self.parts, mn_pt)
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

    def __offset_thread(self):
        _thread.start_new_thread(self.check_offset, (self.datatimeout,))
        _thread.start_new_thread(self._auto_write_offset, ())

    async def download(self):
        self.timeout = ClientTimeout(total=60*60*24, sock_read=2400)
        ssl = False
        ssl_context = self.extra.get("ssl_context")
        if ssl_context:
            self.extra.pop("ssl_context")
            ssl_context.check_hostname = False
            ssl = None
        self.connector = TCPConnector(
            limit=self.tcp_conn, ssl_context=ssl_context, ssl=ssl)
        trust_env = False
        if self.extra.get("proxy_env"):
            trust_env = self.extra.pop("proxy_env")
        async with ClientSession(connector=self.connector, timeout=self.timeout, trust_env=trust_env) as session:
            await self.get_range(session)
            self.__offset_thread()
            self.set_sem(self.threads)
            if os.getenv("RUN_HGET_FIRST") != 'false':
                self.loger.info("File size: %s (%d bytes)",
                                human_size(self.content_length), self.content_length)
                self.loger.info("Starting download %s --> %s",
                                self.url, self.outfile)
            self.loger.debug("Ranges: %s, Sem: %s, Connections: %s, %s", self.parts,
                             self.threads, self.tcp_conn or 100, get_as_part(self.content_length))
            pos = len(
                self.current._identity) and self.current._identity[0]-1 or None
            with tqdm(position=pos, disable=self.quiet, total=int(self.content_length), initial=self.tqdm_init, unit='', ascii=True, unit_scale=True, unit_divisor=1024) as bar:
                tasks = []
                self.rate_limiter.refresh()
                for h_range in self.range_list:
                    s, e = h_range["Range"]
                    if s > e:
                        continue
                    h_range.update(self.headers)
                    task = self.fetch(
                        session, pbar=bar, headers=h_range.copy())
                    tasks.append(task)
                await asyncio.gather(*tasks)

    async def _download_in_executor(self, type_, *args):
        pos = len(
            self.current._identity) and self.current._identity[0]-1 or None
        with tqdm(position=pos, disable=self.quiet, total=int(self.content_length), initial=self.tqdm_init, unit='', ascii=True, unit_scale=True, unit_divisor=1024) as bar:
            self.rate_limiter.refresh()
            with ThreadPoolExecutor(self.threads) as exector:
                tasks = []
                for h_range in self.range_list:
                    s, e = h_range["Range"]
                    if s > e:
                        continue
                    func = getattr(self, "_download_"+type_)
                    task = self.loop.run_in_executor(
                        exector,
                        func,
                        s, e, bar,
                        *args,
                    )
                    tasks.append(task)
                await asyncio.gather(*tasks)

    async def download_ftp(self):
        ftp, self.filepath = login(self.url, timeout=self.datatimeout)
        try:
            await self.get_range(ftp)
        finally:
            ftp.close()
        self.__offset_thread()
        if os.getenv("RUN_HGET_FIRST") != 'false':
            self.loger.info("Logging in as anonymous success")
            self.loger.info("File size: %s (%d bytes)",
                            human_size(self.content_length), self.content_length)
            self.loger.info("Starting download %s --> %s",
                            self.url, self.outfile)
        self.threads = min(self.threads, MAX_FTP_CONNECT)
        self.loger.debug("Ranges: %s, Sem: %s, %s", self.parts,
                         self.threads, get_as_part(self.content_length))
        await self._download_in_executor("ftp")

    def _download_ftp(self, s=0, e=sys.maxsize, pbar=None):
        Range = "bytes=%s-%s" % (s, e)
        self.loger.debug(
            "Start %s %s", currentThread().name, Range)
        s = max(s-1, 0)
        end = e
        if e+1 == self.content_length:
            end += 1
        ftp, filepath = login(self.url, timeout=self.datatimeout)
        try:
            with ftp.transfercmd("RETR " + filepath, rest=s) as conn:
                with open(self.outfile, mode="r+b") as f:
                    f.seek(s)
                    cur = s
                    while True:
                        chunk = conn.recv(self.chunk_size)
                        if not chunk:
                            break
                        if cur+len(chunk) <= end:
                            self.dump_to_file(e, chunk, f, pbar)
                            cur += len(chunk)
                        else:
                            chunk = chunk[:(end-cur)]
                            self.dump_to_file(e, chunk, f, pbar)
                            break
            self.loger.debug(
                "Finished %s %s", currentThread().name, Range)
        finally:
            ftp.close()

    def dump_to_file(self, pos, data, handel, bar):
        self.rate_limiter.wait()
        handel.write(data)
        handel.flush()
        self.offset[pos][-1] += len(data)
        bar.update(len(data))

    async def fetch(self, session, pbar=None, headers=None):
        if headers:
            async with self.sem:
                s, e = headers["Range"]
                headers["Range"] = 'bytes={0}-{1}'.format(s, e)
                self.loger.debug(
                    "Start %s %s", asyncio.current_task().get_name(), headers["Range"])
                proxy, proxy_auth = self.extra.get("proxy"), None
                if self.extra.get("proxy_user") and self.extra.get("proxy_pass") and self.extra.get("proxy"):
                    proxy_auth = BasicAuth(self.extra.get(
                        "proxy_user"), self.extra.get("proxy_pass"))
                async with session.get(self.url, headers=headers, timeout=self.timeout, params=self.extra, proxy=proxy, proxy_auth=proxy_auth) as req:
                    with open(self.outfile, 'r+b') as f:
                        f.seek(s, os.SEEK_SET)
                        async for chunk in req.content.iter_chunked(self.chunk_size):
                            if chunk:
                                self.dump_to_file(e, chunk, f, pbar)
                self.loger.debug(
                    "Finished %s %s", asyncio.current_task().get_name(), headers["Range"])
        else:
            if hasattr(session, "head_object"):
                content_length = await self.loop.run_in_executor(
                    None,
                    self.get_s3_content_length,
                    session
                )
                return content_length
            elif hasattr(session, "size"):
                return session.size(self.filepath)
            else:
                async with session.get(self.url, headers=self.headers, timeout=self.datatimeout, params=self.extra) as req:
                    return req

    def set_sem(self, n):
        self.sem = asyncio.Semaphore(n)

    async def download_s3(self):
        aws_access_key_id = self.extra.get('access_key') or os.getenv(
            "AWS_ACCESS_KEY")
        aws_secret_access_key = self.extra.get('secrets_key') or os.getenv(
            'AWS_SECRETS_KEY')
        if aws_access_key_id and aws_secret_access_key:
            session = client('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             config=Config(max_pool_connections=MAX_S3_CONNECT+1, connect_timeout=self.datatimeout))
        else:
            session = client('s3', config=Config(signature_version=UNSIGNED,
                                                 max_pool_connections=MAX_S3_CONNECT+1, connect_timeout=self.datatimeout))
        u = urlparse(self.url)
        self.bucket, self.key = u.hostname, u.path.lstrip("/")
        await self.get_range(session)
        self.__offset_thread()
        if os.getenv("RUN_HGET_FIRST") != 'false':
            self.loger.info("File size: %s (%d bytes)",
                            human_size(self.content_length), self.content_length)
            self.loger.info("Starting download %s --> %s",
                            self.url, self.outfile)
        self.threads = min(self.threads, MAX_S3_CONNECT)
        self.loger.debug("Ranges: %s, Sem: %s, %s", self.parts,
                         self.threads, get_as_part(self.content_length))
        await self._download_in_executor("s3", session)
        session.close()

    def _download_s3(self, s, e, pbar=None, session=None):
        Range = "bytes=%s-%s" % (s, e)
        self.loger.debug(
            "Start %s %s", currentThread().name, Range)
        response = session.get_object(
            Bucket=self.bucket, Key=self.key, Range=Range)
        with open(self.outfile, 'r+b') as f:
            f.seek(s, os.SEEK_SET)
            for chunk in response["Body"].iter_chunks(self.chunk_size):
                if chunk:
                    self.dump_to_file(e, chunk, f, pbar)
        self.loger.debug(
            "Finished %s %s", currentThread().name, Range)

    def get_s3_content_length(self, session):
        header = session.head_object(Bucket=self.bucket, Key=self.key)
        content_length = header["ContentLength"]
        return content_length

    def run(self):
        self.startime = int(time.time())
        Done = False
        try:
            if self.url.startswith("http"):
                self.loop = asyncio.new_event_loop()
                self.loop.run_until_complete(self.download())
                self.loop.close()
            elif self.url.startswith("ftp"):
                self.ftp = True
                self.loop = asyncio.new_event_loop()
                self.loop.run_until_complete(self.download_ftp())
                self.loop.close()
            elif self.url.startswith("s3"):
                self.loop = asyncio.new_event_loop()
                self.loop.run_until_complete(self.download_s3())
                self.loop.close()
            elif os.path.isfile(self.url) and (self.url.endswith(".ht") or os.path.isfile(self.url+".ht")):
                self.rang_file = self.url.endswith(
                    ".ht") and self.url or self.url+".ht"
                try:
                    self.load_offset()
                except:
                    self.loger.error("Only http/https/s3/ftp urls allowed.")
                    sys.exit(1)
                done, content_length = 0, 0
                for e, s in self.offset.items():
                    l = [e, ] + s
                    done += l[-1]
                    content_length = max(content_length, e+1)
                    sys.stdout.write("\t".join(map(str, l)) + "\n")
                sys.stdout.write("Start time: %s, %s of %s (%.2f%%) finished\n" % (time.strftime(
                    "%F %X", time.localtime(self.startime)), human_size(done), human_size(content_length), float(done)/content_length*100))
                self.offset.clear()
                sys.exit()
            else:
                self.loger.error("Only http/https/s3/ftp urls allowed.")
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

    def _auto_write_offset(self, ivs=10):
        while True:
            time.sleep(ivs)
            self.write_offset()

    def write_offset(self):
        if len(self.offset):
            with self.lock:
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
        if self.current.name == "MainProcess":
            log = logging.getLogger()
        else:
            log = get_logger()
        if self.quiet:
            log.setLevel(logging.ERROR)
        return log

    def check_offset(self, timeout=30):
        time.sleep(5)
        while True:
            o = self._get_data
            time.sleep(timeout)
            if o == self._get_data:
                if len(self.offset):
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
        return deepcopy(self.offset)

    def _exit_without_data(self, timeout):
        if os.environ.get("RUN_MAIN") == "true":
            self.loger.debug(
                "Any data gets in %s sec, Exit 3", timeout)
        else:
            self.loger.error(
                "Any data gets in %s sec, Exit 3", timeout)
        self.write_offset()
        os._exit(3)


def hget(url="", outfile="", threads=Chunk.MAX_AS, quiet=False, tcp_conn=None, timeout=30, **kwargs):
    dn = Download(url=url, outfile=outfile,
                  threads=threads, quiet=quiet, tcp_conn=tcp_conn, timeout=timeout,  **kwargs)
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
