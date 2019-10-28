#!/usr/bin/env python

import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns):
        super(NamazuHandler, self).__init__(patterns=patterns)

    def on_moved(self, event):
        print(f"on_moved:{event}")

    def on_created(self, event):
        print(f"on_created:{event}")

    def on_deleted(self, event):
        print(f"on_deleted:{event}")

    def on_modified(self, event):
        print(f"on_modified:{event}")


def watch(path, extension):
    event_handler = NamazuHandler(["*"+extension])
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    print("started")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


path =  "./sample"
extension = ""

watch(path, extension)
