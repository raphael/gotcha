#!/usr/bin/env ruby

def send(title, url)
  puts title
  puts url
  puts `curl -X #{url} -s -i`
end

send "* Creating project", "POST http://localhost:8000/projects/myproject"
send "* Listing projects", "GET http://localhost:8000/projects"
send "* Creating queue", "POST http://localhost:8000/projects/myproject/queues/myqueue"
send "* Listing queues", "GET http://localhost:8000/projects/myproject/queues"
send "\n* Posting one message", "POST http://localhost:8000/projects/myproject/queues/myqueue/messages -d 'messages=[{\"body\": \"a message\", \"expiresIn\": \"600\"}]'"
send "* Getting message", "GET http://localhost:8000/projects/myproject/queues/myqueue/messages?count=2"
send "\n* Cleaning up", "DELETE http://localhost:8000/projects/myproject"
