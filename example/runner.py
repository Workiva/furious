#!/usr/bin/python

import argparse
import sys


def args():
    parser = argparse.ArgumentParser(description='Run the Furious Examples.')

    parser.add_argument('--gae-sdk-path', metavar='S', dest="gae_lib_path",
                        default="/usr/local/google_appengine",
                        help='path to the GAE SDK')

    parser.add_argument('--url', metavar='U', dest="url", default="",
                        help="the endpoint to run")

    return parser.parse_args()


def setup(options):
    sys.path.insert(0, options.gae_lib_path)

    from dev_appserver import fix_sys_path
    fix_sys_path()


def run(options):
    from google.appengine.tools import appengine_rpc
    from google.appengine.tools import appcfg

    source = 'furious'
    user_agent = appcfg.GetUserAgent()
    server = appengine_rpc.HttpRpcServer(
        'localhost:8080', lambda: ('test@example.com', 'password'), user_agent,
        source, secure=False)

    server._DevAppServerAuthenticate()
    server.Send(options.url, content_type="text/html; charset=utf-8",
                payload=None)


def main():
    options = args()

    setup(options)

    run(options)


if __name__ == "__main__":
    main()
