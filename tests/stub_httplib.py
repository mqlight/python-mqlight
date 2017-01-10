"""
 <copyright                                                             
     notice="copyright-oco-source"                                      
     pids="5724-H72"                                                    
     years="2016"                                                       
     crc="3300043800" >                                                 
                                                                        
     IBM Confidential                                                   
                                                                        
     OCO Source Materials                                               
                                                                        
     5724-H72                                                           
                                                                        
     (C) Copyright IBM Corp. 2016                                       
                                                                        
     The source code for the program is not published                   
     or otherwise divested of its trade secrets,                        
     irrespective of what has been deposited with the                   
     U.S. Copyright Office.                                             
                                                                        
 </copyright>                                                           
"""


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



