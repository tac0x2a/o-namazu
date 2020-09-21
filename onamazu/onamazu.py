
import logging
import schedule
from pathlib import Path
from operator import add
from functools import reduce
from . import (config, csv_handler, mqtt_sender, sweeper, text_handler, watcher)


class ONamazu:
    def __init__(self, root_dir: str, sweep_interval: int, logger=logging.getLogger("o-namazu")):
        self.root_dir = root_dir
        self.sweep_interval = sweep_interval
        self.logger = logger
        self.w = None
        self.initialize_required = True

    def __load_config(self):
        self.logger.info('Load configuration')

        config_map = config.create_config_map(self.root_dir)
        self.logger.info(f'config_map={config_map}')

        if len(config_map) == 0:
            raise Exception("No config file found. Please see README.md")

        return config_map

    def __restart_observers(self, config_map):
        if self.w is not None:
            self.w.stop()
        self.w = watcher.NamazuWatcher(self.root_dir, config_map, self.sample_handler, self.on_config_updated_handler)
        self.w.start()

        schedule.clear()
        schedule.every(self.sweep_interval).seconds.do(lambda: sweeper.sweep(config_map))

    def click(self):
        if self.initialize_required:
            try:
                new_config_map = self.__load_config()
                self.logger.info('Load completed.')

                for path, json in new_config_map.items():
                    self.logger.info(f'Watching: {path}/{json["pattern"]}')

                self.__restart_observers(new_config_map)

                self.logger.info(f"Observe started '{self.root_dir}'")
                self.logger.info("Press 'Ctrl-c' to exit")

            except Exception as e:
                self.logger.error(e, exc_info=e)
                self.logger.error("Error in initialize. Configuration is not applied.")

                if self.w is None:
                    self.logger.error("Failed to first initialize.")
                    raise e

            finally:
                self.initialize_required = False

        self.w.wait(1)
        schedule.run_pending()

    def sample_handler(self, ev):
        self.logger.info(f"{ev.src_path}@{ev.created_at}")
        path = Path(ev.src_path)

        if "mqtt" in ev.config:
            mqtt_config = ev.config["mqtt"]
            mqtt_config = dict(config.DefaultConfig_MQTT, **mqtt_config)

            host = mqtt_config["host"]
            port = mqtt_config["port"]
            topic = mqtt_config["topic"]
            format = mqtt_config["format"]
            length = mqtt_config["length"]

            if format == "csv":
                payload = csv_handler.read(path, ev.config)
                line_count = len(payload.strip().split("\n"))
                self.logger.info(f"CSV Payload is {line_count} lines.")
                payloads = csv_handler.split(payload, length)
            elif format == "text":
                payload = text_handler.read(path, ev.config)
                line_count = len(payload.strip().split("\n"))
                self.logger.info(f"Text Payload is {line_count} lines.")
                payloads = text_handler.split(payload, length)
            else:
                self.logger.error(f"Unsupported format `{format}`, `{path}` has not sent.")
                return

            if len(payloads) <= 0:
                self.logger.info(f"No Payload found. Empty file ? `{path}` was not sent")
                return

            client_id = f"o-namazu_{os.uname()[1]}_{path}"
            mqtt_sender.publish(payloads, client_id, topic, host, port)

            lines = reduce(add, [len(payload.strip().split("\n")) for payload in payloads])
            bytes = reduce(add, [len(payload) for payload in payloads])

            self.logger.info(f"MQTT sent `{path}` done. {len(payloads)} messages, {lines} lines, {bytes} bytes.")

    def on_config_updated_handler(self, ev):
        self.logger.debug(f"{ev.src_path}@{ev.created_at}")
        self.logger.info(f"ConfigFile({ev.src_path}) is changed. All configuration files will be reload.")
        self.initialize_required = True

    def stop(self):
        if self.w is not None:
            self.w.stop()
        schedule.clear()
