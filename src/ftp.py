import os
import sys
import math

from .utils import *

BLOCKSIZE = Chunk.OneK * 10


def download_ftp_by_threads(url, outfile, threads=10, quiet=False):
    content_length = _get_length(url)
    if not os.path.isfile(outfile):
        mkfile(outfile, content_length)
    range_list = _split_range(content_length, threads)
    with tqdm(total=content_length, disable=quiet, initial=0, unit='', ascii=True, unit_scale=True, unit_divisor=1024) as bar:
        with ThreadPoolExecutor(threads) as exector:
            for s, e in range_list:
                exector.submit(download_ftp_range, url, outfile, s, e, bar)


def _split_range(length, threads):
    range_list = []
    step = math.ceil(length/threads)
    for i in range(threads):
        range_list.append([i*step, (i+1)*step])
    if range_list:
        range_list[-1][-1] = length
    return range_list


def _get_length(url):
    u = urlparse(url)
    host, filepath = u.hostname, u.path
    port = u.port or 21
    ftp = FTP()
    ftp.connect(host, port=port, timeout=5)
    ftp.login()
    try:
        length = ftp.size(filepath)
    except:
        raise OSError("get file size error")
    else:
        return length
    finally:
        ftp.close()


def download_ftp_range(url, outfile, s=0, e=sys.maxsize, pbar=None):
    u = urlparse(url)
    host, filepath = u.hostname, u.path
    port = u.port or 21
    ftp = FTP()
    ftp.connect(host, port=port, timeout=5)
    ftp.login()
    ftp.set_pasv(False)
    try:
        with ftp.transfercmd("RETR " + filepath, rest=s) as conn:
            with open(outfile, mode="r+b") as f:
                f.seek(s)
                cur = s
                while True:
                    chunk = conn.recv(BLOCKSIZE)
                    if not chunk:
                        break
                    if cur+len(chunk) <= e:
                        f.write(chunk)
                        pbar.update(len(chunk))
                        cur += len(chunk)
                    else:
                        chunk = chunk[:(e-cur)]
                        f.write(chunk)
                        pbar.update(len(chunk))
                        break
    finally:
        ftp.close()
