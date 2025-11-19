package sqlutil

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ExecBatch executes a batch of queries
func ExecBatch(ctx context.Context, br pgx.BatchResults, sqlCount int) error {
	defer br.Close()

	_, batchErr := ExecuteQueries(ctx, br, sqlCount)

	return batchErr
}

// ExecuteQueries executes all queries in the batch and returns success count and error
func ExecuteQueries(ctx context.Context, br pgx.BatchResults, sqlCount int) (int, error) {
	var batchErr error
	successCount := 0

	for t := 0; t < sqlCount; t++ {
		_, err := br.Exec()
		if err != nil {
			batchErr = errors.Join(batchErr, err)
		} else {
			successCount++
		}
	}

	return successCount, batchErr
}
