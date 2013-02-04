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
import math
import uuid
from furious.job_utils import decode_callbacks
from furious.job_utils import encode_callbacks
from furious.job_utils import function_path_to_reference
from furious.job_utils import get_function_path_and_options
from furious.config import get_configured_persistence_module
persistence_module = get_configured_persistence_module()

