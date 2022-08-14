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
    hget(args.url, outfile, args.num, quite=args.quite,
         tcp_conn=args.connections, timeout=args.timeout)


def main():
    args = parseArg()
    if args.use_reloader:
        autoreloader(_main, args)
    else:
        _main(args)


if __name__ == "__main__":
    main()
