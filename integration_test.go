//go:build integration

package gopqcdcpq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testDBUser             = "test_user"
	testDBPassword         = "test_pass"
	testDBName             = "test_db"
	skipIntegrationTestMsg = "skipping integration test in short mode"
	insertUserSQL          = "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)"
	selectCountByIDSQL     = "SELECT COUNT(*) FROM users WHERE id = $1"
)

// setupPostgresContainer creates a PostgreSQL container for testing
func setupPostgresContainer(ctx context.Context, t *testing.T, name string) (testcontainers.Container, string, int) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":             testDBUser,
			"POSTGRES_PASSWORD":         testDBPassword,
			"POSTGRES_DB":               testDBName,
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		Cmd: []string{
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
		Name: name,
	}

	// Disable Ryuk for podman compatibility
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	return container, host, mappedPort.Int()
}

// createTestTable creates the test table in the database
func createTestTable(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		ALTER TABLE public.users REPLICA IDENTITY DEFAULT;
	`)
	return err
}

// getConnectionString returns the connection string for a database
func getConnectionString(host string, port int) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		testDBUser, testDBPassword, host, port, testDBName)
}

// createTestConfig creates a connector configuration for testing
func createIntegrationTestConfig(sourceHost string, sourcePort int, targetHost string, targetPort int) *config.Connector {
	return &config.Connector{
		App: config.AppConfig{
			Name: "integration-test",
		},
		Server: config.ServerConfig{
			Port: 18080,
		},
		ConnectorConfig: config.ConnectorConfig{
			Publication:       "test_publication",
			Table:             "users",
			Schema:            "public",
			Slot:              "test_slot",
			CreateIfNotExists: true,
			BulkSize:          100,
			Timeout:           5 * time.Second,
			MaxRetries:        3,
			RetryDelay:        100 * time.Millisecond,
			ReplicaIdentity:   "DEFAULT",
		},
		BatchConfig: config.BatchConfig{
			BulkSize:   10,
			Timeout:    1 * time.Second,
			MaxRetries: 3,
			RetryDelay: 100 * time.Millisecond,
		},
		Postgres: config.PostgresConfig{
			Source: config.DatabaseConfig{
				Host:     sourceHost,
				Port:     sourcePort,
				User:     testDBUser,
				Password: testDBPassword,
				DB:       testDBName,
			},
			Target: config.DatabaseConfig{
				Host:     targetHost,
				Port:     targetPort,
				User:     testDBUser,
				Password: testDBPassword,
				DB:       testDBName,
			},
		},
		CDC: cdcconfig.Config{
			Host:     sourceHost,
			Port:     sourcePort,
			Username: testDBUser,
			Password: testDBPassword,
			Database: testDBName,
			Metric: cdcconfig.MetricConfig{
				Port: 18080,
			},
		},
	}
}

// TestIntegrationInsertReplication tests that inserts are replicated from source to target
func TestIntegrationInsertReplication(t *testing.T) {
	if testing.Short() {
		t.Skip(skipIntegrationTestMsg)
	}

	ctx := context.Background()

	// Setup source container
	sourceContainer, sourceHost, sourcePort := setupPostgresContainer(ctx, t, "")
	defer sourceContainer.Terminate(ctx)

	// Setup target container
	targetContainer, targetHost, targetPort := setupPostgresContainer(ctx, t, "")
	defer targetContainer.Terminate(ctx)

	// Create tables
	sourceConnStr := getConnectionString(sourceHost, sourcePort)
	targetConnStr := getConnectionString(targetHost, targetPort)

	err := createTestTable(ctx, sourceConnStr)
	require.NoError(t, err, "failed to create source table")

	err = createTestTable(ctx, targetConnStr)
	require.NoError(t, err, "failed to create target table")

	// Create connector config
	cfg := createIntegrationTestConfig(sourceHost, sourcePort, targetHost, targetPort)

	// Create and start connector
	connector, err := NewConnector(ctx, cfg)
	require.NoError(t, err, "failed to create connector")
	defer connector.Close()

	connectorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go connector.Start(connectorCtx)

	// Wait for connector to be ready
	time.Sleep(3 * time.Second)

	// Insert data into source
	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	require.NoError(t, err)
	defer sourcePool.Close()

	_, err = sourcePool.Exec(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "TestUser", "test@example.com")
	require.NoError(t, err, "failed to insert into source")

	// Wait for replication
	time.Sleep(5 * time.Second)

	// Verify data in target
	targetPool, err := pgxpool.New(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetPool.Close()

	var count int
	err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM users WHERE name = $1", "TestUser").Scan(&count)
	require.NoError(t, err, "failed to query target")
	require.Equal(t, 1, count, "expected 1 row in target, got %d", count)
}

// TestIntegrationUpdateReplication tests that updates are replicated from source to target
func TestIntegrationUpdateReplication(t *testing.T) {
	if testing.Short() {
		t.Skip(skipIntegrationTestMsg)
	}

	ctx := context.Background()

	// Setup containers
	sourceContainer, sourceHost, sourcePort := setupPostgresContainer(ctx, t, "")
	defer sourceContainer.Terminate(ctx)

	targetContainer, targetHost, targetPort := setupPostgresContainer(ctx, t, "")
	defer targetContainer.Terminate(ctx)

	// Create tables
	sourceConnStr := getConnectionString(sourceHost, sourcePort)
	targetConnStr := getConnectionString(targetHost, targetPort)

	err := createTestTable(ctx, sourceConnStr)
	require.NoError(t, err)

	err = createTestTable(ctx, targetConnStr)
	require.NoError(t, err)

	// Insert initial data to source
	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	require.NoError(t, err)
	defer sourcePool.Close()

	_, err = sourcePool.Exec(ctx, insertUserSQL, 100, "OldName", "old@example.com")
	require.NoError(t, err)

	// Insert same data to target (simulating initial sync)
	targetPool, err := pgxpool.New(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetPool.Close()

	_, err = targetPool.Exec(ctx, insertUserSQL, 100, "OldName", "old@example.com")
	require.NoError(t, err)

	// Create and start connector
	cfg := createIntegrationTestConfig(sourceHost, sourcePort, targetHost, targetPort)
	connector, err := NewConnector(ctx, cfg)
	require.NoError(t, err)
	defer connector.Close()

	connectorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go connector.Start(connectorCtx)

	// Wait for connector to be ready
	time.Sleep(3 * time.Second)

	// Update data in source
	_, err = sourcePool.Exec(ctx, "UPDATE users SET name = $1, email = $2 WHERE id = $3", "NewName", "new@example.com", 100)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(5 * time.Second)

	// Verify update in target
	var name, email string
	err = targetPool.QueryRow(ctx, "SELECT name, email FROM users WHERE id = $1", 100).Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "NewName", name)
	require.Equal(t, "new@example.com", email)
}

// TestIntegrationDeleteReplication tests that deletes are replicated from source to target
func TestIntegrationDeleteReplication(t *testing.T) {
	if testing.Short() {
		t.Skip(skipIntegrationTestMsg)
	}

	ctx := context.Background()

	// Setup containers
	sourceContainer, sourceHost, sourcePort := setupPostgresContainer(ctx, t, "")
	defer sourceContainer.Terminate(ctx)

	targetContainer, targetHost, targetPort := setupPostgresContainer(ctx, t, "")
	defer targetContainer.Terminate(ctx)

	// Create tables
	sourceConnStr := getConnectionString(sourceHost, sourcePort)
	targetConnStr := getConnectionString(targetHost, targetPort)

	err := createTestTable(ctx, sourceConnStr)
	require.NoError(t, err)

	err = createTestTable(ctx, targetConnStr)
	require.NoError(t, err)

	// Insert initial data
	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	require.NoError(t, err)
	defer sourcePool.Close()

	_, err = sourcePool.Exec(ctx, insertUserSQL, 200, "ToDelete", "delete@example.com")
	require.NoError(t, err)

	targetPool, err := pgxpool.New(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetPool.Close()

	_, err = targetPool.Exec(ctx, insertUserSQL, 200, "ToDelete", "delete@example.com")
	require.NoError(t, err)

	// Create and start connector
	cfg := createIntegrationTestConfig(sourceHost, sourcePort, targetHost, targetPort)
	connector, err := NewConnector(ctx, cfg)
	require.NoError(t, err)
	defer connector.Close()

	connectorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go connector.Start(connectorCtx)

	// Wait for connector to be ready
	time.Sleep(3 * time.Second)

	// Delete data in source
	_, err = sourcePool.Exec(ctx, "DELETE FROM users WHERE id = $1", 200)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(5 * time.Second)

	// Verify deletion in target
	var count int
	err = targetPool.QueryRow(ctx, selectCountByIDSQL, 200).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "expected row to be deleted from target")
}

// TestIntegrationRetryWorks tests that retry mechanism works by pausing target temporarily.
//
// Test Flow:
//  1. Setup: Create source and target PostgreSQL containers with users table
//  2. Configure: Enable retry with MaxRetries=5, RetryDelay=500ms
//  3. Start: Launch the CDC connector in background
//  4. Stop Target: Stop the target container to simulate database unavailability
//  5. Insert: Write data to source while target is down - CDC captures the change
//     but fails to replicate, triggering the retry mechanism
//  6. Resume Target: Start the target container again
//  7. Verify: Retry mechanism should eventually succeed and data should be in target
func TestIntegrationRetryWorks(t *testing.T) {
	if testing.Short() {
		t.Skip(skipIntegrationTestMsg)
	}

	ctx := context.Background()

	// Step 1: Setup - Create source and target PostgreSQL containers
	sourceContainer, sourceHost, sourcePort := setupPostgresContainer(ctx, t, "")
	defer sourceContainer.Terminate(ctx)

	targetContainer, targetHost, targetPort := setupPostgresContainer(ctx, t, "")
	defer targetContainer.Terminate(ctx)

	// Step 1 (continued): Create users table in both databases
	sourceConnStr := getConnectionString(sourceHost, sourcePort)
	targetConnStr := getConnectionString(targetHost, targetPort)

	err := createTestTable(ctx, sourceConnStr)
	require.NoError(t, err)

	err = createTestTable(ctx, targetConnStr)
	require.NoError(t, err)

	// Step 2: Configure - Configure retry mechanism with specific settings
	// MaxRetries=5: Will attempt up to 5 times before giving up
	// RetryDelay=500ms: Base delay between retries (uses exponential backoff: delay * attempt)
	cfg := createIntegrationTestConfig(sourceHost, sourcePort, targetHost, targetPort)
	cfg.BatchConfig.MaxRetries = 5
	cfg.BatchConfig.RetryDelay = 500 * time.Millisecond
	cfg.BatchConfig.Timeout = 1 * time.Second

	// Step 3: Start - Create and launch the CDC connector
	connector, err := NewConnector(ctx, cfg)
	require.NoError(t, err)
	defer connector.Close()

	connectorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go connector.Start(connectorCtx)

	// Wait for connector to establish replication connection
	time.Sleep(3 * time.Second)

	// Step 4: Stop Target - Simulate database unavailability
	// This causes all write attempts to fail, triggering the retry mechanism
	err = targetContainer.Stop(ctx, nil)
	require.NoError(t, err)
	t.Log("Target container paused - retry should trigger")

	// Step 5: Insert - Write data to source while target is unavailable
	// CDC will capture this change and attempt to replicate it to target
	// Since target is down, the batch execution will fail and retry loop begins
	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	require.NoError(t, err)
	defer sourcePool.Close()

	_, err = sourcePool.Exec(ctx, insertUserSQL, 300, "RetryUser", "retry@example.com")
	require.NoError(t, err)
	t.Log("Data inserted while target is down")

	// Allow some retry attempts to occur while target is still down
	time.Sleep(2 * time.Second)

	// Step 6: Resume Target - Bring the target database back online
	// The next retry attempt should succeed now
	err = targetContainer.Start(ctx)
	require.NoError(t, err)
	t.Log("Target container resumed - retry should succeed")

	// Wait for retry to complete and replication to finish
	time.Sleep(10 * time.Second)

	// Step 7: Verify - Check that retry mechanism successfully replicated the data
	// If retry worked correctly, the row should exist in the target database
	targetPool, err := pgxpool.New(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetPool.Close()

	var count int
	err = targetPool.QueryRow(ctx, selectCountByIDSQL, 300).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "retry should have replicated the data after target came back")
}
