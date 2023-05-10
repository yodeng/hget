#!/usr/bin/env python3
# coding: utf-8


import os
import pdb
import sys

from .src import *


def _main(args):
    log = loger()
    if args.debug:
        log.setLevel(logging.DEBUG)
    outfile = args.output
    if not outfile and args.dir:
        outfile = os.path.join(args.dir, os.path.basename(args.url))
    hget(args.url, outfile, args.num, quiet=args.quiet,
         tcp_conn=args.connections, timeout=args.timeout,
         access_key=args.access_key, secrets_key=args.secrets_key,
         proxy=args.proxy, proxy_user=args.proxy_user, proxy_pass=args.proxy_password, proxy_env=args.use_proxy_env,
         max_speed=args.max_speed)


def main():
    args = parseArg()
    if args.use_reloader:
        autoreloader(_main, args)
    else:
        _main(args)


if __name__ == "__main__":
    main()
