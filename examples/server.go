package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	b := mq.NewBroker(
		mq.WithCallback(tasks.Callback),
		mq.WithBrokerURL(":8081"),
		mq.WithSecurity(true),
	)
	InitializeDefaults(b.SecurityManager())
	// b := mq.NewBroker(mq.WithCallback(tasks.Callback), mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"), mq.WithCAPath("./certs/ca.cert"))
	if err := b.InitializeSecurity(); err != nil {
		panic(err)
	}
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background())
}

// InitializeDefaults adds default permissions, roles, and users for development/testing
func InitializeDefaults(sm *mq.SecurityManager) error {
	permissions := []*mq.Permission{
		{Name: "task.publish", Resource: "task", Action: "publish", Description: "Publish tasks to queues", CreatedAt: time.Now()},
		{Name: "task.consume", Resource: "task", Action: "consume", Description: "Consume tasks from queues", CreatedAt: time.Now()},
		{Name: "queue.manage", Resource: "queue", Action: "manage", Description: "Manage queues", CreatedAt: time.Now()},
		{Name: "admin.system", Resource: "system", Action: "admin", Description: "System administration", CreatedAt: time.Now()},
	}
	err := sm.AddPermissions(permissions...)
	if err != nil {
		return err
	}
	roles := []*mq.Role{
		{Name: "publisher", Description: "Can publish tasks", Permissions: []string{"task.publish"}, CreatedAt: time.Now()},
		{Name: "consumer", Description: "Can consume tasks", Permissions: []string{"task.consume"}, CreatedAt: time.Now()},
		{Name: "admin", Description: "Full system access", Permissions: []string{"task.publish", "task.consume", "queue.manage", "admin.system"}, CreatedAt: time.Now()},
	}
	err = sm.AddRoles(roles...)
	if err != nil {
		return err
	}
	users := []*mq.User{
		{ID: "admin", Username: "admin", Roles: []string{"admin"}, CreatedAt: time.Now(), Password: "admin123"},
		{ID: "publisher", Username: "publisher", Roles: []string{"publisher"}, CreatedAt: time.Now(), Password: "pub123"},
		{ID: "consumer", Username: "consumer", Roles: []string{"consumer"}, CreatedAt: time.Now(), Password: "con123"},
	}
	return sm.AddUsers(users...)
}
