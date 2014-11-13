mqlight.Client.get_id()
^^^^^^^^^^^^^^^^^^^^^^^

Returns the identifier associated with the client. This will either be what
was passed in on the ``Client()`` call or an auto-generated id.

mqlight.Client.get_service()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns the URL of the server to which the client is currently connected
to, or ``None`` if not connected.

mqlight.Client.get_state()
^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns the current state of the client, which will be one of:
'starting', 'started', 'stopping', 'stopped', or 'retrying'.
