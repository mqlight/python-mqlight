
# python-mqlight - high-level API by which you can interact with MQ Light
#
# Copyright 2015-2017 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains the parameter and values used for various
arguments for the API
"""

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1

QOS = (
    QOS_AT_MOST_ONCE,
    QOS_AT_LEAST_ONCE
)
