package gotcha

import (
  "errors"
  "fmt"
  "labix.org/v2/mgo/bson"
  "log"
  "time"
)

// Internal queue structure
type Queue struct {
  ID        bson.ObjectId "_id,omitempty" // ID
  Name      string        "name"          // Name of queue (unique in project)
  ProjectID bson.ObjectId "project"       // Project containing queue
  CreatedAt time.Time     "createdAt"     // Creation timestamp
}

// Queue information returned by APIs
type QueueInfo struct {
  Name        string    `json:"name"`      // Name of queue (unique in project)
  ProjectName string    `json:"project"`   // Name of project containing queue
  CreatedAt   time.Time `json:"createdAt"` // Creation timestamp
  Size        int       `json:"size"`      // Number of messages in queue
}

// Message information returned by APIs
type MessageInfo struct {
  ID               bson.ObjectId `json:"id"`               // ID
  Body             string        `json:"body"`             // Message body (UTF-8)
  QueueName        string        `json:"queue"`            // Name of queue containing message
  ProjectName      string        `json:"project"`          // Name of project containing message
  CreatedAt        time.Time     `json:"createdAt"`        // Creation timestamp
  MessageExpiresAt time.Time     `json:"messageExpiresAt"` // Expiry timestamp
  LeaseExpiresAt   time.Time     `json:"leaseExpiresAt"`   // Timeout of lease in seconds     
}

// Create new queue
func NewQueue(name string, project *Project) (*Queue, error) {
  // Make sure we don't exceed the quota, no need to lock, it's OK if a few extras are created
  info, err := project.Info()
  if err != nil {
    return nil, err
  }
  if info.QueueCount >= MaxQueuesPerProject {
    return nil, errors.New(fmt.Sprintf("Maximum number of queues (%v) reached for project '%v'", MaxQueuesPerProject, project.Name))
  }
  q := Queue{ID: bson.NewObjectId(), Name: name, ProjectID: project.ID, CreatedAt: time.Now().UTC()}
  Mongo.Insert("queue", &q)
  return &q, nil
}

// Retrieve info about the queue
func (q *Queue) Info() (*QueueInfo, error) {
  size, err := Mongo.Count("message", bson.M{"project": q.ProjectID, "queue": q.ID})
  if (err != nil) {
    return nil, err
  }
  project := new(Project)
  err = Mongo.GetId("project", q.ProjectID, &project)
  if (err != nil) {
    return nil, err
  }
  return &QueueInfo{Name: q.Name, ProjectName: project.Name, CreatedAt: q.CreatedAt, Size: size}, nil
}

// Delete queue and all its messages
func (q *Queue) Destroy() error {
  err := q.Clear()
  if err != nil {
    return err
  }
  return Mongo.DestroyId("queue", q.ID)
}

// Return up to 'count' messages from queue and leases them
func (q *Queue) LeaseMessages(count int, timeout time.Duration) (*[]MessageInfo, error) {
  now := time.Now().UTC()
  messages, err := Mongo.FindAndUpdateMessages(bson.M{"project": q.ProjectID, "queue": q.ID, "lease_expires_at": bson.M{"$lt": now}},
    bson.M{"$set": bson.M{"lease_expires_at": now.Add(timeout)}}, "-created_at", count)
  if err != nil {
    return nil, err
  }
  return messageInfos(messages)
}

// Delete all messages from queue
func (q *Queue) Clear() error {
  count, err := Mongo.Destroy("message", bson.M{"project": q.ProjectID, "queue": q.ID})
  log.Printf("Deleted %v messages from queue %v", count, q.ID.Hex())
  return err
}

// Delete given messages by id
// Make sure messages belong to queue first
func (q *Queue) DeleteMessages(messageIds *[]string) error {
  for _, id := range *messageIds {
    m, err := LoadMessage(id)
    if err != nil {
      return err
    }
    if m.QueueID != q.ID {
      return errors.New(fmt.Sprintf("Message with id %v does not belong to queue %v", id, q.Name))
    }
    m.Destroy()
  }
  return nil
}

// Retrieve messages information
// Bulk operation
// IMPORTANT: All messages must be from the same queue!
func messageInfos(messages *[]*Message) (*[]MessageInfo, error) {
  msgs := *messages
  if len(msgs) == 0 {
    res := make([]MessageInfo, 0)
    return &res, nil
  }
  p := new(Project)
  err := Mongo.GetId("project", msgs[0].ProjectID, p)
  if err != nil {
    return nil, err
  }
  q := new(Queue)
  err = Mongo.GetId("queue", msgs[0].QueueID, q)
  if err != nil {
    return nil, err
  }
  infos := make([]MessageInfo, 0, len(msgs))
  for _, m := range msgs {
    infos = append(infos, MessageInfo{ID: m.ID, Body: m.Body, QueueName: q.Name, ProjectName: p.Name, CreatedAt: m.CreatedAt, MessageExpiresAt: m.ExpiresAt, LeaseExpiresAt: m.LeaseExpiresAt})
  }
  return &infos, nil
}


