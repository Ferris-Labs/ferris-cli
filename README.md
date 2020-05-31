Ferris Cli
=====================
[![Downloads](https://pepy.tech/badge/ferris-cli)](https://pepy.tech/project/ferris-cli)

The following library simplifies the process of 
* forwarding Metrics and Task to a Kafka consumer.
* storing and retreiving application properties on the Ferris Platform
* setting up scheduler actions from within the Ferris Platform


# Logging Handler

How to Use
----------

```python

import logging
from ferris_cli.kafka_handler import FerrisKafkaLoggingHandler
class Main:
    def __init__(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s', 
            level=logging.INFO, 
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )
        self.logger = logging.getLogger('simple_example')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        fl = logging.FileHandler("myapp.log")
        self.logger.addHandler(fl)


        kh = KafkaHandler("broker:29092", "pylog")
        kh.setLevel(logging.INFO)
        self.logger.addHandler(kh)
        
    def run(self):
        while True:
            log = input("> ")
            self.logger.info(log)
if __name__ == "__main__":
    main = Main()
    main.run()

```