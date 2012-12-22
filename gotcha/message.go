package gotcha

import (
  "labix.org/v2/mgo/bson"
  "time"
)

// Internal message datastructure
type Message struct {
  ID             bson.ObjectId "_id,omitempty"    // ID
  Body           string        "body"             // Message body (UTF-8 encoded)
  QueueID        bson.ObjectId "queue"            // ID of queue containing message
  ProjectID      bson.ObjectId "project"          // ID of project containing message
  ExpiresAt      time.Time     "expires_at"       // Expiry timestamp (message is deleted after that time)
  CreatedAt      time.Time     "created_at"       // Creation timestamp
  LeaseExpiresAt time.Time     "lease_expires_at" // Lease expiry timestamp if any
}

// Default expiry is set to 7 days
const DefaultMessageExpiry = time.Duration(7 * 24) * time.Hour

// Minimum expiry time for message is set to 1 minute
const MinMessageExpiry = time.Duration(1) * time.Minute

// Maximum expiry time for message is set to 30 days
const MaxMessageExpiry = time.Duration(30 * 24) * time.Hour

// Load message with given Id
func LoadMessage(id string) (*Message, error) {
  m := new(Message)
  err := Mongo.GetId("message", bson.ObjectId(id), m)
  return m, err
}

// Save messages to database
func SaveMessages(messages *[]*Message) error {
  msgs := make([]interface{}, 0, len(*messages))
  for _, msg := range *messages {
    msgs = append(msgs, msg)
  }
  return Mongo.Insert("message", msgs...)
}

// Delete message from database
func (m *Message) Destroy() error {
  return Mongo.DestroyId("message", m.ID)
}

// Whether message is expired
func (m *Message) Expired() bool {
  return m.ExpiresAt.Before(time.Now().UTC())
}
