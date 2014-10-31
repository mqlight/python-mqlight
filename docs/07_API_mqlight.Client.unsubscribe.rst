mqlight.Client.unsubscribe(``topicPattern``, ``[share]``, ``[options]``, ``[callback]``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Stops the flow of messages from a destination to this client. The client's
message callback will not longer be driven when messages arrive, that match the
pattern associated with the destination. Messages may still be stored at the
destination if it has a non-zero time to live value or is shared and is
subscribed to by other clients instances. If the client is not subscribed to a
subscription, as identified by the pattern (and optional) share arguments then
this method will throw a ``UnsubscribedError``.  The pattern and share arguments
will be coerced to type ``str``.  The pattern argument must be present
otherwise this method will throw a ``TypeError``.

* ``topic_pattern`` - (str) Matched against the ``topic_pattern`` specified on
  the ``mqlight.Client.subscribe`` call to determine which destination the
  client will unsubscribed from.
* ``share`` - (str) (optional) Matched against the ``share`` specified on the
  ``mqlight.Client.subscribe`` call to determine which destination the client
  will unsubscribed from.
* ``options`` - (dict) (optional) Properties that determine the behaviour of the
  unsubscribe operation:

  *  **ttl**, (int) (optional) Sets the destination's time to live as part of
     the unsubscribe operation. The default (when this property is not
     specified) is not to change the destination's time to live. When specified
     the only valid value for this property is 0.
* ``callback`` - (function) (optional) callback to be notified when the
  unsubscribe operation completes. The ``callback`` function is passed the
  following arguments:

  *  **error**, (Error) an error object if the callback is being invoked to
     indicate that the unsubscribe call failed. If the unsubscribe call
     completes successfully then the value ``None`` is supplied for this
     argument.
  *  **topic_pattern**, (str) the ``topic_pattern`` argument supplied to the
     corresponding unsubscribe method call.
  *  **share**, (str) the ``share`` argument supplied to the corresponding
     unsubscribe method call (or ``None`` if this parameter was not
     specified).
