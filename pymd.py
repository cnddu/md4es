import sys
import time
#import xmltodict
#import magicdate
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


class MyHandler(PatternMatchingEventHandler):
    patterns=["*.md"]

    def process(self, event):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """



    def on_modified(self, event):
        print("modified => %s" %(event.src_path))
        self.process(event)

    def on_created(self, event):
        print("created => %s" %(event.src_path))
        self.process(event)


if __name__ == '__main__':
    args = sys.argv[1:]
    observer = Observer()
    print(args[0])
    observer.schedule(MyHandler(), path=args[0] if args else '.',recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

