from cloudevents.sdk.event import v03
import json
from ferris_cli.ferris_cli import CloudEventsAPI

event = (
    v03.Event()
    .SetContentType("application/json")
    .SetData('{"name":"john"}')
    .SetEventID("my-id")
    .SetSource("from-galaxy-far-far-away")
    .SetEventTime("tomorrow")
    .SetEventType("cloudevent.greet.you")
)

print(json.dumps(event.Properties()))


print (event.Properties()['source'])
tta = CloudEventsAPI("broker:29092")
tta.send(event)

##print(event.GetData()) 