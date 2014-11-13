mqlight.Client(``service``, [``client_id``], [``security_options``], [``callback``])
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates an MQ Light client instance in ``starting`` state

* `service` - (str, list, function) a string containing
     the URL for the service to connect to, or alternatively a list
     containing a list of URLs to attempt to connect to in turn, or
     alternatively an async function which will be expected to supply the
     service URL(s) to a callback function that will be passed to it whenever
     it is called (in the form ``function(err, service)``). User names and
     passwords may be embedded into the URL (e.g. ``amqp://user:pass@host``).
* `client_id` - (str, default: ``AUTO_[0-9a-f]{7}``) a unique identifier for
     this client. A maximum of one instance of the client (as identified by the
     value of this property) can be connected the an MQ Light server at a given
     point in time. If another instance of the same client connects, then the
     previously connected instance will be disconnected. This is reported, to
     the first client, as a ``ReplacedError`` being emitted as an error event
     and the client transitioning into ``stopped`` state. If the id property is
     not a valid client identifier (e.g. it contains a colon, it is too long,
     or it contains some other forbidden character) then the function will
     throw an ``InvalidArgumentError``
* ``security_options`` - (dict) options for the client. Properties include:

  *  **``property_user``**, (str) (optional), user name for authentication.
     Alternatively, the user name may be embedded in the URL passed via the
     service property. If you choose to specify a user name via this property
     and also embed a user name in the URL passed via the surface argument then
     all the user names must match otherwise an ``InvalidArgumentError`` will be
     thrown.  User names and passwords must be specified together (or not at
     all). If you specify just the user property but no password property an
     ``InvalidArgumentError`` will be thrown.
  *  **``property_password``**, (str) (optional), password for authentication.
     Alternatively, user name may be embedded in the URL passed via the service
     property.
  *  **``ssl_trust_certificate``**, (str) (optional), SSL trust certificate to
     use when authentication is required for the MQ Light server. Only used when
     service specifies the amqps scheme.
  *  **``ssl_verify_name``**, (bool, default: True) (optional), whether or not
     to additionally check the MQ Light server's common name in the certificate
     matches the actual server's DNS name. Only used when the
     ssl_trust_certificate option is specified.
* ``callback`` - (function) (optional) callback that is invoked (indicating
  success) if the client attains ``started`` state, or invoked (indicating
  failure) if the client enters ``stopped`` state before attaining ``started``
  state. The callback function is supplied two arguments, the first being an
  ``Error`` object that is set to ``None`` to indicate success.  The second
  is the instance of ``client``, returned by ``mqlight.Client``, that the
  callback relates to.

Returns a ``Client`` object representing the client instance.

