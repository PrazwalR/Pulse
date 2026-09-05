// Package db builds Cassandra sessions. One contact point is passed in; gocql
// discovers the rest of the ring through gossip — that is the masterless design
// in practice.
package db

import (
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
)

func base(consistency gocql.Consistency) *gocql.ClusterConfig {
	c := gocql.NewCluster(config.ContactPoints...)
	c.Port = config.Port
	c.Consistency = consistency
	c.Timeout = 10 * time.Second
	c.ConnectTimeout = 10 * time.Second
	c.NumConns = 4 // per-host connections shared across the goroutine worker pool
	c.ProtoVersion = 4
	// Only the seed's 9042 is published to the host in the compose file, so peer
	// discovery would hand back unreachable in-container IPs (each blocks for
	// ConnectTimeout). Talk only to the contact point; QUORUM still holds because
	// the coordinator replicates to the other replicas over the cluster network.
	c.DisableInitialHostLookup = true
	if config.Username != "" && config.Password != "" {
		c.Authenticator = gocql.PasswordAuthenticator{Username: config.Username, Password: config.Password}
	}
	return c
}

// Connect opens a session bound to the pulse keyspace.
func Connect(consistency gocql.Consistency) (*gocql.Session, error) {
	c := base(consistency)
	c.Keyspace = config.Keyspace
	return c.CreateSession()
}

// ConnectSystem opens a session without a keyspace (needed to CREATE KEYSPACE).
func ConnectSystem(consistency gocql.Consistency) (*gocql.Session, error) {
	return base(consistency).CreateSession()
}

// ParseConsistency maps ONE/QUORUM/ALL (case-insensitive) to a level, defaulting
// to QUORUM for anything unrecognised.
func ParseConsistency(name string) gocql.Consistency {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "ONE":
		return gocql.One
	case "ALL":
		return gocql.All
	default:
		return gocql.Quorum
	}
}

// DefaultConsistency is the level from config (env CASSANDRA_CONSISTENCY).
func DefaultConsistency() gocql.Consistency {
	return ParseConsistency(config.Consistency)
}
