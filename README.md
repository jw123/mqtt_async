# async_mqtt

A module that provides a means of creating a MQTT client and
connecting it to a broker on an external event loop using asyncio,
rather than using a different thread.

This is based on the loop_asyncio.py example provided as part of the
Eclipse Paho MQTT Python Client (:mod:`paho.mqtt.client`).

See [the Paho MQTT Client](https://github.com/eclipse/paho.mqtt.python)