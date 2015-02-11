Samples
-------

To run the samples, navigate to the `mqlight/samples/` folder.

Usage:

Receiver Sample:
::
usage: recv.py [-h] [-s SERVICE] [-c TRUST_CERTIFICATE] [-t TOPIC_PATTERN]
[-i CLIENT_ID] [--destination-ttl DESTINATION_TTL]
[-n SHARE_NAME] [-f FILE] [-d DELAY] [--verbose VERBOSE]

Connect to an MQ Light server and subscribe to the specified topic.

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, --service SERVICE
                        service to connect to, for example:
                        amqp://user:password@host:5672 or amqps://host:5671 to
                        use SSL/TLS (default: amqp://localhost)
  -c TRUST_CERTIFICATE, --trust-certificate TRUST_CERTIFICATE
                        use the certificate contained in FILE (in PEM or DER
                        format) to validate the identify of the server. The
                        connection must be secured with SSL/TLS (e.g. the
                        service URL must start with "amqps://")
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
                        SHARE_NAME as the share name.
  -f FILE, --file FILE  write the payload of the next message received to FILE
                        (overwriting previous file contents then end. (default
                        is to print messages to stdout)
  -d DELAY, --delay DELAY
                        delay for DELAY seconds each time a message is
                        received. (default: 0)
  --verbose VERBOSE     print additional information about each message.
                        (default: False)

Sender Sample:
::
usage: send.py [-h] [-s SERVICE] [-c TRUST_CERTIFICATE] [-t TOPIC]
[-i CLIENT_ID] [--message-ttl MESSAGE_TTL] [-d DELAY] [-r REPEAT]
[--sequence SEQUENCE] [-f FILE][MESSAGE [MESSAGE ...]]

Send a message to an MQ Light server.

positional arguments:
  MESSAGE               message to be sent (default: ['Hello world !'])

optional arguments:
  -h, --help            show this help message and exit
  -s SERVICE, --service SERVICE
                        service to connect to, for example:
                        amqp://user:password@host:5672 or amqps://host:5671 to
                        use SSL/TLS (default: amqp://localhost)
  -c TRUST_CERTIFICATE, --trust-certificate TRUST_CERTIFICATE
                        use the certificate contained in FILE (in PEM or DER
                        format) to validate the identify of the server. The
                        connection must be secured with SSL/TLS (e.g. the
                        service URL must start with "amqps://")
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
  --sequence SEQUENCE   prefix a sequence number to the message payload,
                        ignored for binary messages (default: False)
  -f FILE, --file FILE  send FILE as binary data. Cannot be specified at the
                        same time as MESSAGE