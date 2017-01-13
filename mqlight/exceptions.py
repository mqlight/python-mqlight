# <copyright
# notice="lm-source-program"
# pids="5724-H72"
# years="2013,2016"
# crc="3250275633" >
# Licensed Materials - Property of IBM
#
# 5725-P60
#
# (C) Copyright IBM Corp. 2013, 2016
#
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
# </copyright>


class MQLightError(Exception):

    # MQ Light base Error
    pass


class InvalidArgumentError(MQLightError):

    """
    A MQLight error indicating that a given argument is incorrect and
    cannot be used. The underlying message will highlight which argument
    is invalid.
    """
    pass


class RangeError(MQLightError):

    """
    A MQLight error indicating that a given argument is not within certain
    values. The underlying message will highlight which argument is out of
    range.
    """
    pass


class NetworkError(MQLightError):

    """
    A MQLight error indicating that an attempted connection or an existing
    connection has failed. This will relate to a network issue and the client
    will treat as recovery and attempt reconnection. The underlying message
    will detail which server it has issue and the reason.
    """
    pass


class NotPermittedError(MQLightError):
    """
    A MQLight error indicates that an operation has been reject by the server
    and is considered an operational error. The underlying message will
    highlight the rejected operation.
    """
    pass


class ReplacedError(MQLightError):

    """
    A MQLight error indicating that the server has detected two clients with
    the same client id are connected. This is not supported and this client
    has been disconnected.
    """
    pass


class SecurityError(MQLightError):

    """
    A MQLight error indicating a failure to connect to the server due to
    a security issue. This may relate to the SASL authentication, or SSL.
    The underlying message will detail which security issue it is and why
    has been rejected.
    """
    pass


class StoppedError(MQLightError):

    """
    A MQLight error indicating a request such as Send, Subscribe and
    Unsubscribed has been requested while the client is not in a started state.
    """
    pass


class SubscribedError(MQLightError):

    """
    A MQLight error indicating that the Subscription request is a
    duplicated subscription and is not supported. The underlying message will
    detail the issue.
    """
    pass


class UnsubscribedError(MQLightError):

    """
    A MQLight error indicating that a request to unsubscribed has been
    rejected as no current subscription can be found. The underlying message
    will detail the issue.
    """
    pass


class InternalError(MQLightError):
    """
    A MQLight error indicating there has been an internal issue. An internal
    module has receive invalid, corrupt or unexcepted data. A FFDC report
    will have been generated with additional diagnostic information.
    """
    pass
