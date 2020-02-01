import paho.mqtt.client as mqtt
import logging

logger = logging.getLogger("mqtt_sender")


class MQTT_Sender:
    def __init__(self):
        pass

    def connect(self, host: str, port):
        self.client = mqtt.Client()                      # クラスのインスタンス(実体)の作成
        self.client.on_connect = self.on_connect         # 接続時のコールバック関数を登録
        self.client.on_disconnect = self.on_disconnect  # 切断時のコールバックを登録
        self.client.on_publish = self.on_publish         # メッセージ送信時のコールバック
        self.client.connect(host, port, 60)

    def send(self, json_data: str, topic: str):
        self.client.publish(topic, json_data)

    def on_connect(self, client, userdata, flag, rc):
        logger.info("connected {}".format(str(rc)))

    def on_disconnect(self, client, userdata, flag, rc):
        logger.info("disconnected")

    def on_publish(self, client, userdata, mid):
        logger.debug(f"Sent_MID: {mid}")
