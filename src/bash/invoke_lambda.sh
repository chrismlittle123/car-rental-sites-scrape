#!/bin/bash

EVENT_NAME=$1
EVENT_PATH="test_events/${EVENT_NAME}.json"

curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @$EVENT_PATH
