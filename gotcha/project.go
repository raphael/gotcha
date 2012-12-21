package gotcha

import (
  "labix.org/v2/mgo/bson"
  "math"
  "time"
)

// A project has an id
type Project struct {
  ID        bson.ObjectId "_id,omitempty"
  Name      string        "name"
  CreatedAt time.Time     "created_at"
}

// Project info exported to API
type ProjectInfo struct {
  Name        string    `json:"name"`
  QueueCount  int       `json:"queueCount"`
  CreatedAt   time.Time `json:"createdAt"`
}

// Maximum number of queues a single project can hold
const MaxQueuesPerProject = 100000

// List all projects
func ListProjects() (*[]Project, error) {
  ps := make([]Project, 0, 10)
  err := Mongo.Get("project", bson.M{}, math.MaxInt32, &ps)
  return &ps, err
}

// Create new project
func NewProject(name string) (*Project, error) {
  p := Project{ID: bson.NewObjectId(), Name: name, CreatedAt: time.Now().UTC()}
  err := Mongo.Insert("project", &p)
  return &p, err
}

// Load project by name, return nil if not found
func LoadProject(name string) (*Project, error) {
  p := new(Project)
  err := Mongo.GetOne("project", bson.M{"name": name}, p)
  return p, err
}

// Return all queues from given project
func (p *Project) Queues() (*[]Queue, error) {
  qs := make([]Queue, 0)
  err := Mongo.Get("queue", bson.M{"project": p.ID}, MaxQueuesPerProject, &qs)
  return &qs, err
}

// Return queue with given name from given project
func (p *Project) Queue(name string) (*Queue, error) {
  q := new(Queue)
  err := Mongo.GetOne("queue", bson.M{"project": p.ID, "name": name}, q)
  return q, err
}

// Return info about this project
func (p *Project) Info() (*ProjectInfo, error) {
  count, err := Mongo.Count("queue", bson.M{"project": p.ID})
  if err != nil {
    return nil, err
  }
  return &ProjectInfo{Name: p.Name, QueueCount: count, CreatedAt: p.CreatedAt}, nil
}

// Destroy project and all that it contains
func (p *Project) Destroy() error {
  qs := make([]*Queue, 0)
  if err := Mongo.Get("queue", bson.M{"project": p.ID}, MaxQueuesPerProject, &qs); err != nil {
    return err
  } else {
    for _, q := range qs {
      if err := q.Destroy(); err != nil {
        return err
      }
    }
  }
  return Mongo.DestroyId("project", p.ID)
}
