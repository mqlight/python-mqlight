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
