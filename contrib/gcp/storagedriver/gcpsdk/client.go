// Package gcpsdk provides a storagedriver.Client implementation backed by the
// official Google Cloud Storage Go client (cloud.google.com/go/storage).
// Import this package alongside storagedriver when using that client; use a
// different adapter package for alternative GCS clients.
//
// NOTE: Experimental
package gcpsdk

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"

	"go.temporal.io/sdk/contrib/gcp/storagedriver"
)

type gcsClient struct {
	client *storage.Client
}

// NewClient creates a storagedriver.Client backed by a cloud.google.com/go/storage
// client.
//
// NOTE: Experimental
func NewClient(client *storage.Client) storagedriver.Client {
	return &gcsClient{client: client}
}

func (c *gcsClient) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	w := c.client.Bucket(bucket).Object(key).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("failed to write object: %w", err)
	}
	err := w.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

func (c *gcsClient) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.client.Bucket(bucket).Object(key).Attrs(ctx)
	// Here it doesn't really matter the reason why the object was
	// not found as long as the actual call was successful.
	if errors.Is(err, storage.ErrObjectNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check object: %w", err)
	}
	return true, nil
}

func (c *gcsClient) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {

	r, err := c.client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		// There is no way to distinguish between a not found object
		// and a missing bucket without making an extra call. Likely
		// if we only make the extra call on error it should be fine,
		// but for now not doing anything.
		//
		// TODO: disambiguate for diagnostics?
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (c *gcsClient) Describe() map[string]string {
	return nil
}
