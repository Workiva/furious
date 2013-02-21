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

#collection of context callbacks for different occasions
import logging


def small_aggregated_results_success_callback(idx, result):
    """
    args:
        id: the job id
        result: the combined output of all the Async functions

    A Context success callback is passed the id of the job
    and the result of the job(the combined output of all the functions).
    In this case it checks the real time the job took
    At this point, the result can be no larger then 1MB
    """
    logging.info("job done id: {0}".format(idx))
