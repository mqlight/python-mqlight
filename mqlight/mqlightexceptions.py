"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2013,2014"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2014

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
class MQLightError(Exception):

    """
    MQ Light base Error
    """
    pass


class InvalidArgumentError(MQLightError):

    """
    A subtype of MQLightError defined by the MQ Light client. It is considered a
    programming error. The underlying cause for this error are the parameter
    values passed into a method.
    """
    pass

class RangeError(MQLightError):

    """
    A subtype of MQLightError defined by the MQ Light client. The underlying
    cause for this error are the parameter values passed into a method are not
    within certain values
    """

class NetworkError(MQLightError):

    """
    This is a subtype of MQLightError defined by the MQ Light client. It is
    considered an operational error. NetworkError is passed to an application
    if the client cannot establish a network connection to the MQ Light server,
    or if an established connection is broken.
    """
    pass


class ReplacedError(MQLightError):

    """
    This is a subtype of MQLightError defined by the MQ Light client. It is
    considered an operational error. ReplacedError is thrown to signify that an
    instance of the client has been replaced by another instance that connected
    specifying the exact same client id.
    """
    pass

class LocalReplacedError(ReplacedError):
    """
    Special type of ReplacedError thrown by an invalidated Client instance. An
    invaildated Client instance is one where the application has created another
    Client instance with the same id, which replaces it.
    """
    def __init__(self):
        self.msg = 'Client is Invalid. Application has created a ' + \
            'second Client instance with the same id'


class SecurityError(MQLightError):

    """
    This is a subtype of MQLightError defined by the MQ Light client. It is
    considered an operational error. SecurityError is thrown when an operation
    fails due to a security related problem.
    """
    pass


class StoppedError(MQLightError):

    """
    This is a subtype of MQLight Error defined by the MQ Light client. It is
    considered a programming error - but is unusual in that, in some
    circumstances, a client may reasonably expect to receive StoppedError as a
    result of its actions and would typically not be altered to avoid this
    condition occurring.  StoppedError is thrown by methods which require
    connectivity to the server (e.g. send, subscribe) when they are invoked
    while the client is in the stopping or stopped states.
    """
    pass


class SubscribedError(MQLightError):

    """
    This is a subtype of MQLightError defined by the MQ Light client. It is
    considered a programming error. SubscribedError is thrown from the
    client.subscribe(...) method call when a request is made to subscribe to a
    destination that the client is already subscribed to.
    """
    pass


class UnsubscribedError(MQLightError):

    """
    This is a subtype of MQLightError defined by the MQ Light client. It is
    considered a programming error. UnsubscribedError is thrown from the
    client.unsubscribe(...) method call when a request is made to unsubscribe
    from a destination that the client is not subscribed to.
    """
    pass
