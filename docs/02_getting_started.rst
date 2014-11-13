Getting Started
---------------

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
