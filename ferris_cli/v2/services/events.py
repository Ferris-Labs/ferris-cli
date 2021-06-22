import os
import json
import uuid
from datetime import datetime
import logging
from kafka import KafkaProducer
from cloudevents.sdk.event.v1 import Event


class FerrisEvents:

    def send(self, event_type, event_source, data, topic=None, reference_id=None):

        if not topic:
            topic = os.environ.get("DEFAULT_TOPIC", "ferris.events.topic")

        date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        evt = (
            Event()
            .SetEventID(uuid.uuid1().hex)
            .SetContentType("application/json")
            .SetSource(f"{event_source}")
            .SetEventType(f"{event_type}")
            .SetEventTime(date_time)
            .SetData(json.dumps(data))
        )

        if reference_id:
            evt.SetSubject(reference_id)

        resp = self.broker.send(
            topic,
            evt.Properties()
        )

        logging.getLogger("ferris.apps.web.cloudevents").debug("Response from broker.send: %s ", str(resp))
        logging.getLogger("ferris.apps.web.cloudevents").debug("Sent event to %s topic with %s data", topic, json.dumps(evt.Properties()))

        return True


