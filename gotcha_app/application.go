package main

import (
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "gotcha"
	"io"
  "io/ioutil"
  "labix.org/v2/mgo/bson"
  "log"
	"net/http"
  "os"
  "path/filepath"
	"github.com/bmizerany/pat"
  "launchpad.net/goyaml"
  "strconv"
  "strings"
  "time"
)

// Default settings
func defaultSettings() map[string]string{
  return map[string]string{
    "port":          "8000",
    "environment":   "development",
    "mongoHost":     "localhost",
    "mongoUser":     "",
    "mongoPassword": "",
  }
}

// Maximum number of messages that can be enqueued at once
const MaxEnqueueCount = 100

// Maximum number of messages that can be retrieved at once
const MaxLeaseCount = 100

// Default lease timeout
const DefaultMessageTimeout = time.Duration(1) * time.Minute //60 * 1000 * 1000 * 1000)

// Minimum timeout for lease is 10 seconds
const MinMessageTimeout = time.Duration(10) * time.Second // * 1000 * 1000 * 1000)

// Maximum timeout for lease is 24 hours
const MaxMessageTimeout = time.Duration(24) * time.Hour //24 * 60 * 60 * 1000 * 1000 * 1000)

// Current settings
var globalSettings map[string]string

// Load configuration settings and setup database connection
func init() {
  globalSettings := make(map[string]string)
  var confFile string
  flag.StringVar(&confFile, "config", "config.yml", "Path to config file")
  flag.Parse()
  if confFile, err := filepath.Abs(confFile); err != nil {
    log.Printf("Cannot find configuration file '%s', using default settings", confFile)
  } else if raw, err := ioutil.ReadFile(confFile); err != nil {
    log.Printf("[%s] Cannot load configuration file '%s', using default settings", os.Getpid(), confFile)
  } else {
    err := goyaml.Unmarshal(raw, &globalSettings)
    if err != nil {
      log.Printf("Cannot load configuration settings: %s", err)
    }
  }
  for setting, value := range defaultSettings() {
    if _, ok := globalSettings[setting]; !ok {
      globalSettings[setting] = value
    }
  }
  msg, err := goyaml.Marshal(&globalSettings)
  if err != nil {
    log.Fatalf("Could not log settings: %s", err)
  }
  log.Printf("Startup settings:\n%s", msg)

  gotcha.StartSession(globalSettings["mongoHost"], globalSettings["mongoUser"],
    globalSettings["mongoPassword"], globalSettings["environment"])
}

// Entry point, load routes and start server
func main() {
	m := pat.New()
	m.Post("/projects/:projectName", http.HandlerFunc(createProject))
	m.Get("/projects/:projectName", http.HandlerFunc(showProject))
	m.Del("/projects/:projectName", http.HandlerFunc(deleteProject))
	m.Get("/projects/:projectName/queues", http.HandlerFunc(listQueues))
  m.Post("/projects/:projectName/queues/:queueName", http.HandlerFunc(createQueue))
  m.Get("/projects/:projectName/queues/:queueName", http.HandlerFunc(showQueue))
  m.Del("/projects/:projectName/queues/:queueName", http.HandlerFunc(deleteQueue))
  m.Post("/projects/:projectName/queues/:queueName/clear", http.HandlerFunc(clearQueue))
  m.Post("/projects/:projectName/queues/:queueName/messages", http.HandlerFunc(addMessages))
  m.Get("/projects/:projectName/queues/:queueName/messages", http.HandlerFunc(getMessages))
  m.Post("/projects/:projectName/queues/:queueName/messages/delete", http.HandlerFunc(deleteMessages))

	http.Handle("/", m)
	err := http.ListenAndServe(":8000", nil)
	if err != nil {
		log.Fatalf("Could not start server: %s", err)
	}
}

/* 
 POST /projects/:projectName

 Create new project with given name, idempotent

 Parameters
   - none

 Response
   - code: 204
   - body: none
*/
func createProject(w http.ResponseWriter, req *http.Request) {
  name := req.URL.Query().Get(":projectName")
  if _, err := gotcha.NewProject(name); err != nil {
    http.Error(w, fmt.Sprintf("Failed to create project: %s", err), 422)
  } else {
    w.WriteHeader(204)
  }
}

/* 
 GET /projects/:projectName

 Retrieve information about project with given name

 Parameters
   - none

 Response
   - code: 200
   - body (JSON): {name:"foo", queues_count:10, created_at:"2009-11-10 23:00:00 +0000 UTC"}

 Not found error
   - code: 404
   - body: Project not found
*/
func showProject(w http.ResponseWriter, req *http.Request) {
  p, err := findProject(w, req)
  if err != nil {
    http.Error(w, "Project not found", 404)
    return
  }
  i, err := p.Info()
  if err != nil {
    http.Error(w, fmt.Sprintf("Failed to load project details: %s", err), 422)
    return
  }
  sendResponse(w, i)
}

/* 
 DELETE /projects/:projectName

 Delete project with given name

 Parameters
   - none

 Response
   - code: 204
   - body: none
   
 Not found error
   - code: 404
   - body: Project not found

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func deleteProject(w http.ResponseWriter, req *http.Request) {
  p, err := findProject(w, req)
  if err != nil {
    http.Error(w, "Project not found", 404)
  } else {
    if err := p.Destroy(); err != nil {
      http.Error(w, fmt.Sprintf("Failed to delete project: %s", err), 422)
    } else {
      w.WriteHeader(204)
    }
  }
}

/* 
POST /projects/:projectName/queues/:queueName

 Create new queue with given name in given project, idempotent

 Parameters
   - none

 Response
   - code: 204
   - body: none
*/
func createQueue(w http.ResponseWriter, req *http.Request) {
  if p, err := findProject(w, req); err != nil {
    http.Error(w, "Project not found", 404)
    return
  } else {
    name := req.URL.Query().Get(":queueName")
    if _, err := gotcha.NewQueue(name, p); err != nil {
      http.Error(w, fmt.Sprintf("Failed to create queue: %s", err), 422)
    } else {
      w.WriteHeader(204)
    }
  }
}

/* 
 GET /projects/:projectName/queues

 Retrieve all queues from given project

 Parameters
   - none

 Response
   - code: 200
   - body (JSON): [{name:"foo", size:10, created_at:"2009-11-10 23:00:00 +0000 UTC"}, ...]

 Misc. error
   - code: 422
   - body: <Error message>
*/
func listQueues(w http.ResponseWriter, req *http.Request) {
  if p, err := findProject(w, req); err != nil {
    http.Error(w, "Project not found", 404)
    return
  } else {
    if qs, err := p.Queues(); err != nil {
      http.Error(w, fmt.Sprintf("Failed to load queues: %s", err), 422)
    } else {
      infos := make([]gotcha.QueueInfo, 0, len(*qs))
      for _, q := range *qs {
        if i, err := q.Info(); err != nil {
          http.Error(w, fmt.Sprintf("Failed to retrieve queue details: %s", err), 422)
        } else {
          infos = append(infos, *i)
        }
      }
      sendResponse(w, &infos)
    }
  }
}

/* 
 GET /projects/:projectName/queues/:queueName

 Retrieve information about given queue

 Parameters
   - none

 Response
   - code: 200
   - body (JSON): {name:"foo", size:10, created_at:"2009-11-10 23:00:00 +0000 UTC"}

 Not found error
   - code: 404
   - body: Queue not found

 Misc. error
   - code: 422
   - body: <Error message>
*/
func showQueue(w http.ResponseWriter, req *http.Request) {
  if q, err := findQueue(w, req); err != nil {
    http.Error(w, "Queue not found", 404)
  } else {
    if i, err := q.Info(); err != nil {
      http.Error(w, fmt.Sprintf("Failed to retrieve queue details: %s", err), 422)
    } else {
      sendResponse(w, i)
    }
  }
}

/* 
 DELETE /projects/:projectName/queues/:queueName

 Delete queue with given name

 Parameters
   - none

 Response
   - code: 204
   - body: none
   
 Not found error
   - code: 404
   - body: Queue not found

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func deleteQueue(w http.ResponseWriter, req *http.Request) {
  if q, err := findQueue(w, req); err != nil {
    http.Error(w, "Queue not found", 404)
  } else {
    if err := q.Destroy(); err != nil {
      http.Error(w, fmt.Sprintf("Failed to delete queue: %s", err), 422)
    } else {
      w.WriteHeader(204)
    }
  }
}

/* 
 POST /projects/:projectName/queues/:queueName/clear

 Delete all messages from given queue

 Parameters
   - none

 Response
   - code: 204
   - body: none
   
 Not found error
   - code: 404
   - body: Queue not found

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func clearQueue(w http.ResponseWriter, req *http.Request) {
  if q, err := findQueue(w, req); err != nil {
    http.Error(w, "Queue not found", 404)
  } else {
    if err := q.Clear(); err != nil {
      http.Error(w, fmt.Sprintf("Failed to clear queue: %s", err), 422)
    } else {
      w.WriteHeader(204)
    }
  }
}

/* 
 POST /projects/:projectName/queues/:queueName/messages

 Add messages to queue (100 max in a single request)

 The messages must be set in the "messages" form value serialized in a JSON array
 Each message must be a hash consisting of the following key value pairs:
   - body:       required, contains the UTF-8 encoded message body
   - expires_in: optional, contains the amount of time the message must be kept
                 in the queue before it is either read or discarded, default is
                 7 days

 The response contains one id per message in the "ids" header. ids are comma
 separated.

 Parameters (Form-Encoded value containing JSON array)
   - messages: [{body: "...", expires_in: 6000}, ...]

 Response
   - code: 201
   - header: ids: "12fasd1", ...
   
 Not found error
   - code: 404
   - body: Queue not found

 Badly formed request error
   - code: 400
   - body: <Error message>

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func addMessages(w http.ResponseWriter, req *http.Request) {
  q, err := findQueue(w, req)
  if err != nil {
    http.Error(w, "Queue not found", 404)
    return
  }
  if err := req.ParseForm(); err != nil {
    http.Error(w, "Badly formed request (invalid form data)", 400)
    return
  }
  messagesJson := req.Form.Get("messages")
  if messagesJson == "" {
    http.Error(w, "Badly formed request (no 'messages' form value)", 400)
    return
  }
  messages := make([]map[string]string, 0, 5)
  err = json.Unmarshal([]byte(messagesJson), &messages)
  if err != nil {
    http.Error(w, "Badly formed request ('messages' value contains malformed JSON)", 400)
    return
  }
  if len(messages) > MaxEnqueueCount {
    http.Error(w, fmt.Sprintf("Cannot enqueue more than %s messages in one request", MaxEnqueueCount), 400)
    return
  }
  internalMsgs := make([]gotcha.Message, 0, len(messages))
  now := time.Now().UTC()
  for _, m := range messages {
    body := m["body"]
    if body == "" {
      http.Error(w, "Badly formed request ('messages' contains a message with no 'body' value)", 400)
      return
    }
    expiresIn, err := extractDuration(m["expires_in"], gotcha.MinMessageExpiry, gotcha.MaxMessageExpiry, gotcha.DefaultMessageExpiry)
    if err != nil {
      http.Error(w, fmt.Sprintf("Badly formed request: %s (expires_in)", err), 400)
      return
    }
    internalMsgs = append(internalMsgs, gotcha.Message{ID: bson.NewObjectId(), Body: body, QueueID: q.ID, ProjectID: q.ProjectID,
                                                ExpiresAt: now.Add(expiresIn), CreatedAt: now})
  }
  err = gotcha.SaveMessages(&internalMsgs)
  if err != nil {
    http.Error(w, fmt.Sprintf("Failed to enqueue messages: %s", err), 422)
    return
  }
  ids := make([]string, 0, len(internalMsgs))
  for _, m := range internalMsgs {
    ids = append(ids, string(m.ID))
  }
  w.Header().Add("ids", strings.Join(ids, ","))
  w.WriteHeader(201)
}

// Extract duration from form value
// If value is nil then use provided default value
// If value is not an integer then return an error
// If value is not is the sepcified min/max range then return an error
func extractDuration(val interface{}, min, max, def time.Duration) (time.Duration, error) {
  if val == nil || val == "" {
    return def, nil
  }
  intVal := 0
  switch val.(type) {
  case int:
    intVal = val.(int)
  case string:
    strVal := val.(string)
    var err error
    intVal, err = strconv.Atoi(strVal)
    if err != nil {
      return time.Duration(0), errors.New(fmt.Sprintf("Invalid duration value '%s'", val))
    }
  }
  return time.Duration(intVal) * time.Second, nil
}

/* 
 GET /projects/:projectName/queues/:queueName/messages?count=20&timeout=30

 Lease messages from queue (100 max in a single request)

 The response contains a JSON encoded hash with two key/pairs:
   - messages: Contains actual messages, details below
   - timeout:  Timeout value specified in GET request if any

 Each message is a hash consisting of the following key value pairs:
   - id:         Unique message id
   - body:       UTF-8 encoded message body
   - timeout:    Maximum amount of time the message can be leased before 
                 it is put back in the queue

 Parameters (Form-Encoded array containing JSON data)
 - count: optional, Number of messages to lease (100 max), default to 1
 - timeout: optional, Lease timeout, messages that are not deleted before timeout
            get placed back in queue, default to value specified when enqueueing

 Response
   - code: 201
   - header: ids: "12fasd1", ...
   
 Not found error
   - code: 404
   - body: Queue not found

 Badly formed request error
   - code: 400
   - body: <Error message>

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func getMessages(w http.ResponseWriter, req *http.Request) {
  q, err := findQueue(w, req)
  if err != nil {
    http.Error(w, "Queue not found", 404)
    return
  }
  countStr := req.URL.Query().Get("count")
  count := 0
  if countStr == "" {
    count = 1
  } else {
    count, err = strconv.Atoi(countStr)
    if err != nil {
      http.Error(w, fmt.Sprintf("Invalid count value '%s' (must be an integer)", countStr), 400)
      return
    }
  }
  timeout, err := extractDuration(req.URL.Query().Get("timeout"), MinMessageTimeout, MaxMessageTimeout, DefaultMessageTimeout)
  if err != nil {
    http.Error(w, fmt.Sprintf("Invalid timeout value '%s' (must be an integer <= %s >= %s)", timeout, MaxMessageTimeout.Seconds(), MinMessageTimeout.Seconds()), 400)
    return
  }
  messages, err := q.LeaseMessages(count, timeout)
  if err != nil {
    http.Error(w, fmt.Sprintf("Failed to lease messages (%s)", err), 400)
    return
  }
  sendResponse(w, messages)
  if messages != nil {
    b, err := json.Marshal(*messages)
    if err != nil {
      http.Error(w, "Failed to serialize response", 500)
      return
    }
    io.WriteString(w, string(b))
  }
}

/* 
 POST /projects/:projectName/queues/:queueName/messages/delete

 Delete messages from queue

 ids should be be in a JSON encoded array in the "messageIds" form value

 Parameters (Form-Encoded array containing JSON data)
 - messageIds: required, Ids of messages to be deleted

 Response
   - code: 204
   - header: none
   
 Not found error
   - code: 404
   - body: Queue not found

 Badly formed request error
   - code: 400
   - body: <Error message>

 Misc error (e.g. lost connection to MongoDB)
   - code: 422
   - body: <Error message>
*/
func deleteMessages(w http.ResponseWriter, req *http.Request) {
  q, err := findQueue(w, req)
  if err != nil {
    http.Error(w, "Queue not found", 404)
    return
  }
  if err := req.ParseForm(); err != nil {
    http.Error(w, "Badly formed request (invalid form data)", 400)
    return
  }
  messageIdsJson := req.Form.Get("messageIds")
  if messageIdsJson == "" {
    http.Error(w, "Badly formed request (no 'messageIds' form value)", 400)
    return
  }
  messageIds := make([]string, 0)
  err = json.Unmarshal([]byte(messageIdsJson), &messageIds)
  if err != nil {
    http.Error(w, "Badly formed request ('messageIds' value contains malformed JSON)", 400)
    return
  }
  err = q.DeleteMessages(&messageIds)
  if err != nil {
    http.Error(w, fmt.Sprintf("Could not delete all messages: %s", err), 422)
    return
  }
  w.WriteHeader(204)
}

// Helper method to send document or error in http response
func sendResponse(w http.ResponseWriter, doc interface{}) {
  if b, err := json.Marshal(doc); err != nil {
    log.Printf("**ERROR: Failed to serialize %s: %s", doc, err)
    http.Error(w, "Failed to serialize response", 500)
  } else {
    io.WriteString(w, string(b))
  }
}

// Helper method to find project and return error if not found
func findProject(w http.ResponseWriter, req *http.Request) (*gotcha.Project, error) {
  name := req.URL.Query().Get(":projectName")
  p, err := gotcha.LoadProject(name)
  if err != nil {
    return nil, errors.New(fmt.Sprintf("Project with name '%s' not found", name))
  }
  return p, nil
}

// Helper method to find queue and return error if not found
func findQueue(w http.ResponseWriter, req *http.Request) (*gotcha.Queue, error) {
  p, err := findProject(w, req)
  if err != nil {
    return nil, err
  }
  name := req.URL.Query().Get(":queueName")
  q, err := p.Queue(name)
  if err != nil {
    return nil, errors.New(fmt.Sprintf("Queue with name '%s' not found", name))
  }
  return q, nil
}
