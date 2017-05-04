
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

# successful
OK = 200
#CREATED = 201
#ACCEPTED = 202
#NON_AUTHORITATIVE_INFORMATION = 203
#NO_CONTENT = 204
#RESET_CONTENT = 205
#PARTIAL_CONTENT = 206
#MULTI_STATUS = 207
#IM_USED = 226

class HTTPResponse(object):
    def __init__(self, status, data):
        self._status = status
        self._data = data
    def _get_status(self):
        return self._status
    status = property(_get_status)
    def read(self):
        return self._data

RESPONSE = HTTPResponse(0, None)

class HTTPConnection(object):
    def __init__(self, host):
        pass

    def request(self, method, url, body=None, headers={}):
        pass

    def getresponse(self, buffering=False):
        global RESPONSE
        return RESPONSE

    @staticmethod
    def set_response(response, data):
        global RESPONSE
        RESPONSE = HTTPResponse(response, data)



class HTTPSConnection(object):
    def __init__(self, host):
        pass

    def request(self, method, url, body=None, headers={}):
        pass

    def getresponse(self, buffering=False):
        return RESPONSE

class HTTPException(Exception):
    pass
