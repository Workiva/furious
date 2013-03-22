#
# Copyright 2013 WebFilings, LLC
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
import logging


def dummy_success_callback(idx, results):
    return [idx, results]


def sub_context_empty_target_example():
    from furious.context import Context
    ctx = Context()
    return ctx


def dummy_leaf_combiner(results):
    logging.debug("dummy_leaf_combiner been called!")
    return [result for result in results]


def dummy_internal_vertex_combiner(results):
    logging.debug("dummy_internal_vertex_combiner been called!")
    return results
