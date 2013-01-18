#!/usr/bin/python
"""
This script will aid in running the examples and "integration" tests included
with furious.

To run:

    python example/runner.py workflow

This will hit the /workflow url, causing the "workflow" example to run.
"""

import argparse
import sys


def args():
    """Add and parse the arguments for the script.

    url: the url of the example to run
    gae-sdk-path: this allows a user to point the script to their GAE SDK
                  if it's not in /usr/local/google_appengine.
    """
    parser = argparse.ArgumentParser(description='Run the Furious Examples.')

    parser.add_argument('--gae-sdk-path', metavar='S', dest="gae_lib_path",
                        default="/usr/local/google_appengine",
                        help='path to the GAE SDK')

    parser.add_argument('url', metavar='U', default="", nargs=1,
                        help="the endpoint to run")

    return parser.parse_args()


def setup(options):
    """Grabs the gae_lib_path from the options and inserts it into the first
    index of the sys.path. Then calls GAE's fix_sys_path to get all the proper
    GAE paths included.

    :param options:
    """
    sys.path.insert(0, options.gae_lib_path)

    from dev_appserver import fix_sys_path
    fix_sys_path()


def run(options):
    """Run the passed in url of the example using GAE's rpc runner.

    Uses appengine_rpc.HttpRpcServer to send a request to the url passed in
    via the options.

    :param options:
    """
    from google.appengine.tools import appengine_rpc
    from google.appengine.tools import appcfg

    source = 'furious'

    # use the same user agent that GAE uses in appcfg
    user_agent = appcfg.GetUserAgent()

    # Since we're only using the dev server for now we can hard code these
    # values. This will need to change and accept these values as variables
    # when this is wired up to hit appspots.
    server = appengine_rpc.HttpRpcServer(
        'localhost:8080', lambda: ('test@example.com', 'password'), user_agent,
        source, secure=False)

    # if no url is passed in just use the top level.
    url = "/"
    if options.url:
        url += options.url[0]

    # use the dev server authentication for now.
    server._DevAppServerAuthenticate()

    # send a simple GET request to the url
    server.Send(url, content_type="text/html; charset=utf-8",
                payload=None)


def main():
    """Send a request to the url passed in via the options using GAE's rpc
    server.
    """
    options = args()

    setup(options)

    run(options)


if __name__ == "__main__":
    main()
