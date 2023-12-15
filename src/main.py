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
    ssl_context = None
    if args.cacert and args.cert and args.key:
        ssl_context = ssl.create_default_context(cafile=args.cacert)
        ssl_context.load_cert_chain(args.cert, args.key)
    cookies = args.cookies and dict(
        [l.strip().split("=", 1) for l in args.cookies.split(";")]) or {}
    headers = args.user_agent and {hdrs.USER_AGENT: args.user_agent} or {}
    kw = {
        "url": args.url,
        "outfile": outfile,
        "threads": args.num,
        "quiet": args.quiet,
        "tcp_conn": args.connections,
        "timeout": args.timeout,
        "access_key": args.access_key,
        "secrets_key": args.secrets_key,
        "proxy": args.proxy,
        "proxy_user": args.proxy_user,
        "proxy_pass": args.proxy_password,
        "proxy_env": args.use_proxy_env,
        "max_speed": args.max_speed,
        "ssl_context": ssl_context,
        "cookies": cookies,
        "headers": headers
    }
    if args.requests:
        kw.update(input_args=args, func=MultiRequests)
    hget(**kw)


def main():
    args = parseArg()
    if not args.noreload:
        autoreloader(_main, args)
    else:
        _main(args)


if __name__ == "__main__":
    main()
