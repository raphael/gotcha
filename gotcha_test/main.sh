#!/bin/bash -e
echo Creating project
curl -X POST http://localhost:8000/projects/myproject

echo Creating queue
curl -X POST http://localhost:8000/projects/myproject/queues/myqueue

echo Listing queues
curl -X GET http://localhost:8000/projects/myproject/queues && echo

echo Posting one message
curl -X POST http://localhost:8000/projects/myproject/queues/myqueue/messages --form messages='[{body: "a message", expires_in: 600}]' && echo

echo Getting message
curl -X GET http://localhost:8000/projects/myproject/queues/myqueue/messages && echo

echo Cleaning up
curl -X DELETE http://localhost:8000/projects/myproject
