package gotcha

/*
  This file encapsulates access to MongoDB

  Usage:
    StartSession("localhost", "user", "password", "development")
    newProject := new(Project){ID: bson.NewObjectId(), Name: "myproject"}
    Mongo.Insert("project", newProject)
    project := new(Project)
    Mongo.GetOne("project", bson.M{"name": "foo"}, &project)
    ...
    Mongo.DestroyId("projects", project.ID)
    Mongo.Close()

  Available methods on "Mongo":
    Insert: Insert document in given collection
    GetId: Read document from id from given collection
    Get: Retrieve documents matching given query from given collection
    GetOne: Retrieve first document matching given query from given collection
    Count: Count number of documents matching given query
    DeleteId: Delete document with given id from given collection
    Delete: Delete all documents matching given query from given collection
*/

import (
  "labix.org/v2/mgo"
  "labix.org/v2/mgo/bson"
  "log"
)

// Current session if any
// Create new session by calling startSession
var Mongo *session

// A mongoDB session, includes reference to database
type session struct {
  mgoSession *mgo.Session
  db         *mgo.Database
}

// Creates new session to given host for given environment
// Name of database is inferred from environment
func StartSession(host, user, pass, env string) error {
  // Close any existing session
  if Mongo != nil {
    log.Print("Closing existing MongoDB session prior to opening a new one")
    Mongo.Close()
    Mongo = nil
  }

  // Initialize connection and login
  s, err := mgo.Dial(host)
  if err != nil {
    log.Printf("**ERROR: Could not connect to MongoDB: %v", err)
    return err
  }
  db := s.DB(env)
  if user != "" && pass != "" {
    if err := db.Login(user, pass); err != nil {
      log.Printf("**ERROR: Could not login to MongoDB: %v", err)
      return err
    }
  }
  s.SetMode(mgo.Monotonic, true)

  // Setup database indices if needed
  if err := createIndex(db, "project", []string{"name"}, true); err != nil {
    return err
  }
  if err := createIndex(db, "queue", []string{"project", "name"}, true); err != nil {
    return err
  }
  if err := createIndex(db, "message", []string{"project", "queue", "lease_expires_at"}, false); err != nil {
    return err
  }
  if err := createIndex(db, "message", []string{"created_at"}, false); err != nil {
    return err
  }

  // Finally, initialize 'Mongo'
  Mongo = &session{mgoSession: s, db: db}

  return nil
}

// Helper method to create indices
func createIndex(db *mgo.Database, col string, keys []string, unique bool) error {
    index := mgo.Index{
    Key: keys,
    Unique: unique,
    DropDups: false,
    Background: false,
    Sparse: false,
  }
  c := db.C(col)
  err := c.EnsureIndex(index)
  if err != nil {
    log.Printf("**ERROR: Failed to create %v index on %v collection: %v", keys, col, err)
    return err
  }
  return nil
}

// Close session
func (s *session) Close() {
  s.mgoSession.Close()
}

// Insert one document
func (s *session) Insert(col string, doc interface{}) error {
  c := s.db.C(col)
  err := c.Insert(doc)
  if err != nil {
    log.Printf("**ERROR: Could not insert document '%v' in collection %v: %v", doc, col, err)
  }
  return err
}

// Get document from id
func (s *session) GetId(col string, id bson.ObjectId, doc interface{}) error {
  c := s.db.C(col)
  err := c.FindId(id).One(doc)
  if err != nil {
    log.Printf("**ERROR: Could not lookup document with id %v from collection %v: %v", id.Hex(), col, err)
  }
  return err
}

// Retrieve multiple documents at once using given query
// Limit result set to 'len(docs)' documents
func (s *session) Get(col string, query bson.M, maxCount int, docs interface{}) error {
  c := s.db.C(col)
  err := c.Find(query).Limit(maxCount).All(docs)
  if err == mgo.ErrNotFound {
    err = nil // It's ok not to find anything matching the query in this case (it's not for GetOne)
  }
  if err != nil  {
    log.Printf("**ERROR: Failed to run query %v in collection %v: %v", query, col, err)
  }
  return err
}

// Retrieve one document using given query
func (s *session) GetOne(col string, query bson.M, doc interface{}) error {
  c := s.db.C(col)
  err := c.Find(query).One(doc)
  if err != nil {
    log.Printf("**ERROR: Failed to run query %v in collection %v: %v", query, col, err)
  }
  return err
}

// Count documents using given query
func (s *session) Count(col string, query bson.M) (int, error) {
  c := s.db.C(col)
  count, err := c.Find(query).Count()
  if err != nil {
    log.Printf("**ERROR: Could not count documents with query %v from collection %v: %v", query, col, err)
  }
  return count, err
}

// Update multiple messages and retrieve them
// Each update on each message is atomic with the query used to retrieve it
// This uses MongoDB 'findAndModify' which can only act on one document at a time
// so this loops until the desired count is updated/retrieved
func (s *session) FindAndUpdateMessages(query bson.M, update bson.M, sort string, maxCount int) (*[]*Message, error) {
  c := s.db.C("message")
  change := mgo.Change{Update: update, ReturnNew: true}
  res := make([]*Message, 0, maxCount)
  for i := 0; i < maxCount; i++ {
    m := new(Message)
    _, err := c.Find(query).Sort(sort).Apply(change, m)
    if err == mgo.ErrNotFound {
      break
    } else if err != nil {
      return nil, err
    }
    res = append(res, m)
  }
  return &res, nil
}

// Delete
func (s *session) DestroyId(col string, id bson.ObjectId) error {
  c := s.db.C(col)
  err := c.RemoveId(id)
  if err != nil {
    log.Printf("**ERROR: Failed to delete %v from collection %v: %v", id, col, err)
  }
  return err
}

// Delete all documents that match given query
// Return number of deleted documents
func (s *session) Destroy(col string, query bson.M) (int, error) {
  c := s.db.C(col)
  info, err := c.RemoveAll(query)
  if err != nil {
    log.Printf("**ERROR: Failed to delete with query %v from collection %v: %v", query, col, err)
  }
  return info.Removed, err
}

