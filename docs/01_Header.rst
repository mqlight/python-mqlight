Welcome to mqlight's documentation!
===================================

MQ Light is designed to allow applications to exchange discrete pieces of
information in the form of messages. This might sound a lot like TCP/IP
networking, and MQ Light does use TCP/IP under the covers, but MQ Light takes
away much of the complexity and provides a higher level set of abstractions to
build your applications with.

This python module provides the high-level API by which you can interact
with the MQ Light runtime.

See https://developer.ibm.com/messaging/mq-light/ for more details.

Getting Started
---------------

Prerequisites
^^^^^^^^^^^^^

TODO

Usage
^^^^^

.. code:: python

    import mqlight


Then create some instances of the client object to send and receive messages:

.. code:: python

    recv_client = mqlight.Client('amqp://localhost')

    topic_pattern = 'public'
    def subscribe(err):
        recv_client.subscribe(topic_pattern)
    def messages(data, delivery, options):
        print 'Recv: ', data
    recv_client.add_listener(mqlight.STARTED, subscribe)
    recv_client.add_listener(mqlight.MESSAGE, message)

    send_client = mqlight.Client('amqp://localhost')

    topic = 'public'
    def send(err):
        def sent(err, data):
            send_client.stop()
        send_client.send(topic, 'Hello World!', sent)
    send_client.add_listener(mqlight.STARTED, send)


API
---
