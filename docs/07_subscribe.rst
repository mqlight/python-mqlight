mqlight.Client.subscribe(``topic_pattern``, [``share``], [``options``], [``callback``])
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Subscribes the client to a destination, based on the supplied ``topic_pattern``
and ``share`` arguments. The client throw a ``SubscribedError`` if a call is
made to ``client.subscribe(...)`` and the client is already associated with the
destination (as determined by the pattern and share arguments). It will throw
a ``StoppedError`` if the client has not been started prior to calling this
function.

The ``topic_pattern`` argument is matched against the ``topic`` that messages
are sent to, allowing the messaging service to determine whether a particular
message will be delivered to a particular destination, and hence the
subscribing client.

* ``topic_pattern`` - (str) used to match against the ``topic`` specified when a
  message is sent to the messaging service. A pattern can contain any character
  in the Unicode character set, with ``#`` representing a multilevel wildcard
  and ``+`` a single level wildcard as described
  `here
  <https://developer.ibm.com/messaging/mq-light/wildcard-topicpatterns/>`_).
  .
* ``share`` - (str) (optional) name for creating or joining a shared
  destination for which messages are anycast between connected subscribers. If
  omitted defaults to a private destination (e.g. messages can only be received
  by a specific instance of the client).
* ``options`` - (dict) (optional) additional options for the subscribe
  operation. Supported options are:

  * **auto_confirm**, (bool) (optional) When set to True (the default) the
    client will automatically confirm delivery of messages when all of the
    listeners registered for the client's ``message`` event have returned.
    When set to ``False``, application code is responsible for confirming the
    delivery of messages using the ``confirm_delivery`` method, passed via
    the ``delivery`` argument of the listener registered for ``message``
    events. ``auto_confirm`` is only applicable when the ``qos`` property is
    set to 1.  The ``qos`` property is described later.
  * **credit**, (int) The maximum number of unconfirmed messages a client
    can have before the server will stop sending new messages to the client
    and require that it confirms some of the outstanding message deliveries in
    order to receive more messages.  The default for this property is 1024. If
    specified the value will be coerced to a ``int`` and must be finite
    and >= 0, otherwise a ``RangeError`` will be thrown.
  * **qos**, (int) The quality of service to use for delivering messages to
    the subscription.  Valid values are: 0 to denote at most once (the
    default), and 1 for at least once. A ``RangeError`` will be thrown for
    other value.
  * **ttl**, (int) A time-to-live value, in seconds, that is applied
    to the destination that the client is subscribed to. If specified the
    value will be coerced to a ``int``, which must be finite and >= 0,
    otherwise a ``RangeError`` will be thrown. This value will replace any
    previous value, if the destination already exists. Time to live starts
    counting down when there are no instances of a client subscribed to a
    destination.  It is reset each time a new instance of the client
    subscribes to the destination. If time to live counts down to zero then MQ
    Light will delete the destination by discarding any messages held at the
    destination and not accruing any new messages. The default value for this
    property is 0 - which means the destination will be deleted as soon as
    there are no clients subscribed to it.
* ``callback`` - (function) (optional) callback to be notified when the
  subscribe operation completes. The ``callback`` function is passed the
  following arguments:

  * **error**, (Error) an error object if the callback is being invoked to
    indicate that the subscribe call failed. If the subscribe call completes
    successfully then the value ``None`` is supplied for this argument.
  * **topic_pattern**, (str) the ``topic_pattern`` argument supplied to the
    corresponding subscribe method call.
  * **share**, (str) the ``share`` argument supplied to the corresponding
    subscribe method call (or ``None`` if this parameter was not specified).

Returns the ``Client`` object that the subscribe was called on. ``message``
events will be emitted when messages arrive.
