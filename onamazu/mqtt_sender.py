import paho.mqtt.publish as pub
import logging

logger = logging.getLogger("mqtt_sender")


def publish(payloads: list, client_id: str, topic: str, hostname: str, port: int, qos: int = 0, retain: bool = False):
    # msg = {'topic':"<topic>", 'payload':"<payload>", 'qos':<qos>, 'retain':<retain>}
    messages = [{'topic': topic, 'payload': p, 'qos': qos, 'retain': retain} for p in payloads]
    pub.multiple(messages, hostname, port, client_id)
