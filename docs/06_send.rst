mqlight.Client.send(``topic``, ``data``, [``options``], [``callback``])
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sends the value, specified via the ``data`` argument to the specified topic.
str and lst values will be sent and received as-is.

* ``topic`` - (str) the topic to which the message will be sent.
  A topic can contain any character in the Unicode character set.
* ``data`` - (str, lst) the message body to be sent
* ``options`` - (dict) (optional) additional options for the send operation.
  Supported options are:

  *  **qos**, (int) (optional) The quality of service to use when sending the
     message. 0 is used to denote at most once (the default) and 1 is used for
     at least once. If a value which is not 0 and not 1 is specified then this
     method will throw a ``RangeError``
  *  **ttl**, (int) (optional) A time to live value for the message in
     seconds. MQ Light will endeavour to discard, without delivering, any
     copy of the message that has not been delivered within its time to live
     period. The default time to live is 604800 seconds (7 days).
     The value supplied for this argument must be greater than zero and finite,
     otherwise a ``RangeError`` will be thrown when this method is called.
* ``callback`` - (function) The callback argument is optional if the qos
  property of the options argument is omitted or set to 0 (at most once). If
  the qos property is set to 1 (at least once) then the callback argument is
  required and a ``InvalidArgumentError`` is thrown if it is omitted. The
  callback will be notified when the send operation completes and is passed the
  following arguments:

  *  **error**, (Error) an error object if the callback is being invoked to
     indicate that the send call failed. If the send call completes successfully
     then the value ``None`` is supplied for this argument.
  *  **topic**, (str) the ``topic`` argument supplied to the corresponding
     send method call.
  *  **data**, (dict) the ``data`` argument supplied to the corresponding
     send method call.
  *  **options**, (dict) the ``options`` argument supplied to the corresponding
     send method call.

Returns ``True`` if this message was sent, or is the next to be sent.

Returns ``False`` if the message was queued in user memory, due to either a
backlog of messages, or because the client was not in a connected state.
When the backlog of messages is cleared, the ``drain`` event will be emitted.
