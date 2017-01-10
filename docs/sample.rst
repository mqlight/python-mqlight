Samples
-------

To run the samples, navigate to the `mqlight/samples/` folder.


.. index:: Sample;Receiver

Receiver Sample:

usage: recv.py [-h] [-s SERVICE] [-t TOPIC_PATTERN] [-i CLIENT_ID]
               [--destination-ttl DESTINATION_TTL] [-n SHARE_NAME] [-f FILE]
               [-d DELAY] [--verbose] [-c FILE] [--client-certificate  FILE]
               [--client-key FILE] [--client-key-passphrase PASSPHRASE]
               [--no-verify-name SSL_VERIFY_NAME]

Connect to an MQ Light server and subscribe to the specified topic.

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, --service SERVICE
                        service to connect to, for example:
                        amqp://user:password@host:5672 or amqps://host:5671 to
                        use SSL/TLS (default: None)
  -t TOPIC_PATTERN, --topic-pattern TOPIC_PATTERN
                        subscribe to receive messages matching TOPIC_PATTERN
                        (default: public)
  -i CLIENT_ID, --id CLIENT_ID
                        the ID to use when connecting to MQ Light (default:
                        send_[0-9a-f]{7})
  --destination-ttl DESTINATION_TTL
                        set destination time-to-live to DESTINATION_TTL
                        seconds (default: None)
  -n SHARE_NAME, --share-name SHARE_NAME
                        optionally, subscribe to a shared destination using
                        SHARE_NAMEas the share name.
  -f FILE, --file FILE  write the payload of the next message received to FILE
                        (overwriting previous file contents then end. (default
                        is to print messages to stdout)
  -d DELAY, --delay DELAY
                        delays the confirmation for DELAY seconds each time a
                        message is received. (default: 0)
  --verbose             print additional information about each message.

ssl arguments:
  -c FILE, --trust-certificate FILE
                        use the certificate contained in FILE (in PEM or DER
                        format) to validate the identify of the server. The
                        connection must be secured with SSL/TLS (e.g. the
                        service URL must start with "amqps://")
  --client-certificate  FILE
                        use the certificate contained in FILE (in PEM format)
                        to supply the identity of the client. The connection
                        must be secured with SSL/TLS
  --client-key FILE     use the private key contained in FILE (in PEM format)
                        for encrypting the specified client certificate
  --client-key-passphrase PASSPHRASE
                        use PASSPHRASE to access the client private key
  --no-verify-name SSL_VERIFY_NAME
                        specify to not additionally check the server's common
                        name in the specified trust certificate matches the
                        actual server's DNS name


.. index:: Sample;Sender

Sender Sample:

usage: send.py [-h] [-s SERVICE] [-t TOPIC] [-i CLIENT_ID]
               [--message-ttl MESSAGE_TTL] [-d DELAY] [-r REPEAT] [--sequence]
               [-f FILE] [--verbose] [-c FILE] [--client-certificate  FILE]
               [--client-key FILE] [--client-key-passphrase PASSPHRASE]
               [--no-verify-name SSL_VERIFY_NAME]
               [MESSAGE [MESSAGE ...]]


Send a message to a MQ Light server.

positional arguments:
  MESSAGE               message to be sent (default: ['Hello world!'])

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, --service SERVICE
                        service to connect to, for example:
                        amqp://user:password@host:5672 or amqps://host:5671 to
                        use SSL/TLS (default: None)
  -t TOPIC, --topic TOPIC
                        send messages to topic TOPIC (default: public)
  -i CLIENT_ID, --id CLIENT_ID
                        the ID to use when connecting to MQ Light (default:
                        send_[0-9a-f]{7})
  --message-ttl MESSAGE_TTL
                        set message time-to-live to MESSAGE_TTL seconds
                        (default: None)
  -d DELAY, --delay DELAY
                        add NUM seconds delay between each request (default:
                        0)
  -r REPEAT, --repeat REPEAT
                        send messages REPEAT times, if REPEAT <= 0 then repeat
                        forever (default: 1)
  --sequence            prefix a sequence number to the message payload,
                        ignored for binary messages
  -f FILE, --file FILE  send FILE as binary data. Cannot be specified at the
                        same time as MESSAGE
  --verbose             print additional information about each message.

ssl arguments:
  -c FILE, --trust-certificate FILE
                        use the certificate contained in FILE (in PEM or DER
                        format) to validate the identify of the server. The
                        connection must be secured with SSL/TLS (e.g. the
                        service URL must start with "amqps://")
  --client-certificate  FILE
                        use the certificate contained in FILE (in PEM format)
                        to supply the identity of the client. The connection
                        mustbe secured with SSL/TLS
  --client-key FILE     use the private key contained in FILE (in PEM format)
                        for encrypting the specified client certificate
  --client-key-passphrase PASSPHRASE
                        use PASSPHRASE to access the client private key
  --no-verify-name SSL_VERIFY_NAME
                        specify to not additionally check the server's common
                        name in the specified trust certificate matches the
                        actual server's DNS name


usage: uiworkout.py [-h] [-s SERVICE] [-v] [-c FILE]
                    [--client-certificate  FILE] [--client-key FILE]
                    [--client-key-passphrase PASSPHRASE] [--no-verify-name]


.. index:: Sample;UIWorkout

UIWorkout Sample:

Send and receives a number of messages to a MQ Light server.

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, --service SERVICE
                        service to connect to, for example:
                        amqp://user:password@host:5672 or amqps://host:5671 to
                        use SSL/TLS (default: amqp://localhost)
  -v, --verbose         Increase the verbose output of the sample

ssl arguments:
  -c FILE, --trust-certificate FILE
                        use the certificate contained in FILE (in PEM or DER
                        format) to validate the identify of the server. The
                        connection must be secured with SSL/TLS (e.g. the
                        service URL must start with "amqps://")
  --client-certificate  FILE
                        use the certificate contained in FILE (in PEM format)
                        to supply the identity of the client. The connection
                        mustbe secured with SSL/TLS
  --client-key FILE     use the private key contained in FILE (in PEM format)
                        for encrypting the specified client certificate
  --client-key-passphrase PASSPHRASE
                        use PASSPHRASE to access the client private key
  --no-verify-name      specify to not additionally check the server's common
                        name in the specified trust certificate matches the
                        actual server's DNS name