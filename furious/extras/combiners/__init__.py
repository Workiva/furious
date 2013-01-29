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

def lines_combiner(results):
    """
    args:
        results
    returns:
        joins all the results together into a string
        if one of the results is None, "" will be used instead
    """
    return reduce(lambda x,y: x+"".join(y) if isinstance(y,list)
    else x+(y or ""),results,"")
