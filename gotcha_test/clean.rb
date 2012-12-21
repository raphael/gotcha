#!/usr/bin/env ruby

def send(title, url)
  puts title
  puts url
  puts `curl -X #{url} -s -i`
end

send "\n* Cleaning up", "DELETE http://localhost:8000/projects/myproject"
