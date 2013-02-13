#
# Copyright 2012 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import time
import json
import logging

import webapp2

from webapp2_extras import jinja2


class BatcherViewHandler(webapp2.RequestHandler):
    """Batcher view to insert tasks and view the stats.

    url: host/batcher
    """

    @webapp2.cached_property
    def jinja2(self):
        # Returns a Jinja2 renderer cached in the app registry.
        return jinja2.get_jinja2(app=self.app)

    def render_response(self, _template, **context):
        # Renders a template and writes the result to the response.
        rv = self.jinja2.render_template(_template, **context)
        self.response.write(rv)

    def get(self):
        context = {}

        self.render_response('batcher.html', **context)


class BatcherHandler(webapp2.RequestHandler):
    """Handler for inserting batch messages. Takes urlparams of color, value
    and count. The color is just a tag and value is an incrementing number for
    that tag. The count is the number of messages with that payload to insert.
    """

    def get_params(self):
        color = self.request.GET['color']
        value = self.request.GET['value']
        count = int(self.request.GET['count'])

        assert color
        assert value
        assert count

        return color.strip(), value.strip(), count

    def get(self):
        from furious import context
        from furious.batcher import Message
        from furious.batcher import MessageProcessor

        try:
            color, value, count = self.get_params()
        except (KeyError, AssertionError):
            response = {
                "success": False,
                "message": "Invalid parameters."
            }
            self.response.write(json.dumps(response))
            return

        payload = {
            "color": color,
            "value": value,
            "timestamp": time.mktime(datetime.datetime.utcnow().timetuple())
        }

        tag = "color"

        # create a context to insert multiple Messages
        with context.new() as ctx:
            # loop through the count adding a task to the context per increment
            for _ in xrange(count):
                # insert the message with the payload
                ctx.add(Message(task_args={"payload": payload, "tag": tag}))

        # insert a processor to fetch the messages in batches
        # this should always be inserted. the logic will keep it from inserting
        # too many processors
        processor = MessageProcessor(
            target=process_messages, args=(tag,), tag=tag,
            task_args={"countdown": 0})
        processor.start()

        response = {
            "success": True,
            "message": "Task inserted successfully with %s" % (payload,)
        }

        self.response.write(json.dumps(response))


class BatcherStatsHandler(webapp2.RequestHandler):
    """Handler for returing the stats to the client. Returns as a json payload.
    Pulls the stats from memcache.
    """

    def get(self):
        from google.appengine.api import memcache

        stats = memcache.get('color')
        stats = stats if stats else json.dumps(get_default_stats())
        self.response.write(stats)


def process_messages(tag, retries=0):
    """Processes the messages pulled fromm a queue based off the tag passed in.
    Will insert another processor if any work was processed or the retry count
    is under the max retry count. Will update a aggregated stats object with
    the data in the payload of the messages processed.

    :param tag: :class: `str` Tag to query the queue on
    :param retry: :class: `int` Number of retries the job has processed
    """
    from furious.batcher import bump_batch
    from furious.batcher import MESSAGE_DEFAULT_QUEUE
    from furious.batcher import MessageIterator
    from furious.batcher import MessageProcessor

    from google.appengine.api import memcache

    # since we don't have a flag for checking complete we'll re-insert a
    # processor task with a retry count to catch any work that may still be
    # filtering in. If we've hit our max retry count we just bail out and
    # consider the job complete.
    if retries > 5:
        logging.info("Process messages hit max retry and is exiting")
        return

    # create a message iteragor for the tag in batches of 500
    message_iterator = MessageIterator(tag, MESSAGE_DEFAULT_QUEUE, 500)

    client = memcache.Client()

    # get the stats object from cache
    stats = client.gets(tag)

    # json decode it if it exists otherwise get the default state.
    stats = json.loads(stats) if stats else get_default_stats()

    work_processed = False

    # loop through the messages pulled from the queue.
    for message in message_iterator:
        work_processed = True

        value = int(message.get("value", 0))
        color = message.get("color").lower()

        # update the total stats with the value pulled
        set_stats(stats["totals"], value)

        # update the specific color status via the value pulled
        set_stats(stats["colors"][color], value)

    # insert the stats back into cache
    json_stats = json.dumps(stats)

    # try and do an add first to see if it's new. We can't trush get due to
    # a race condition.
    if not client.add(tag, json_stats):
        # if we couldn't add than lets do a compare and set to safely
        # update the stats
        if not client.cas(tag, json_stats):
            raise Exception("Transaction Collision.")

    # bump the process batch id
    bump_batch(tag)

    if work_processed:
        # reset the retries as we've processed work
        retries = 0
    else:
        # no work was processed so increment the retries
        retries += 1

    # insert another processor
    processor = MessageProcessor(
        target=process_messages, args=("colors",),
        kwargs={'retries': retries}, tag="colors")

    processor.start()


def set_stats(stats, value):
    """Updates the stats with the value passed in.

    :param stats: :class: `dict`
    :param value: :class: `int`
    """
    stats["total_count"] += 1
    stats["value"] += value
    stats["average"] = stats["value"] / stats["total_count"]

    # this is just a basic example and not the best way to track aggregation.
    # for max and min old there are cases where this will not work correctly.
    if value > stats["max"]:
        stats["max"] = value

    if value < stats["min"] or stats["min"] == 0:
        stats["min"] = value


def get_default_stats():
    """Returns a :class: `dict` of the default stats structure."""

    default_stats = {
        "total_count": 0,
        "max": 0,
        "min": 0,
        "value": 0,
        "average": 0,
        "last_update": None,
    }

    return {
        "totals": default_stats,
        "colors": {
            "red": default_stats.copy(),
            "blue": default_stats.copy(),
            "yellow": default_stats.copy(),
            "green": default_stats.copy(),
            "black": default_stats.copy(),
        }
    }
