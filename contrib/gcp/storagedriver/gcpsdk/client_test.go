package gcpsdk_test

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"go.temporal.io/sdk/contrib/gcp/storagedriver"
	"go.temporal.io/sdk/contrib/gcp/storagedriver/gcpsdk"
)

const (
	// Enable integration tests by setting this variable to "true", which will
	// run the same tests against a real bucket. Note that "bucketName" must be
	// set to the real bucket if this is enabled, and the tests will actually
	// write against that bucket, possibly incurring costs.
	//
	// To create and configure a service account for local integration tests:
	//
	//	PROJECT_ID="<your-project-id>"
	//	SA_NAME="temporal-gcs-driver"
	//	SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
	//	BUCKET="<bucketName>"
	//
	//	gcloud iam service-accounts create "${SA_NAME}" \
	//	  --display-name="Temporal GCS storage driver" \
	//	  --project="${PROJECT_ID}"
	//
	//	gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
	//	  --member="serviceAccount:${SA_EMAIL}" \
	//	  --role=roles/storage.objectUser
	//
	// Then to authenticate tests use a credentials file:
	//
	//  GCS_TEST_CREDENTIALS="~/.tmp-external-storage/sa-key.json"
	//	gcloud iam service-accounts keys create "${GCS_TEST_CREDENTIALS}" \
	//	  --iam-account="${SA_EMAIL}"
	//
	// You can also impersonate if you want to avoid key files:
	//
	//	gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
	//	  --member="user:YOUR_EMAIL@example.com" \
	//	  --role=roles/iam.serviceAccountTokenCreator
	//
	//	gcloud auth application-default login \
	//	  --impersonate-service-account="${SA_EMAIL}"
	integrationEnvVar = "ENABLE_GCS_INTEGRATION_TESTS"
	bucketName        = "ozoniuss-temporal-external-storage"
	missingBucketName = "shouldntexist-on-gcp-sakbsdkuabdasuda"
)

// newFakeGCS starts an in-process fake GCS server and returns a
// storagedriver.Client backed by a real *storage.Client pointing at it.
// The returned cleanup is registered with t.Cleanup.
func newFakeGCS(t *testing.T, bucket string) storagedriver.Client {
	t.Helper()
	if bucket == "" {
		t.Fatal("empty bucket name")
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Host: "127.0.0.1",
		Port: 0,
	})
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	return gcpsdk.NewClient(server.Client())
}

// newRealGCS creates a connection against a real cloud storage bucket. Note
// that tests treat the bucket as if it were empty, and do not clear the files
// once the tests complete.
//
// TODO: maybe cache client for tests.
func newRealGCS(t *testing.T, bucket string) storagedriver.Client {
	t.Helper()

	if os.Getenv(integrationEnvVar) == "" {
		t.Skipf("set %s=1 to run real-bucket integration tests", integrationEnvVar)
	}

	if bucket == "" {
		t.Fatal("empty bucket name")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var credsPath string
	if p := os.Getenv("GCS_TEST_CREDENTIALS"); p != "" {
		credsPath = p
	}

	var opts []option.ClientOption

	// Tests may be run without a creds file, so only setting when the env var
	// is set.
	if len(credsPath) != 0 {
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, credsPath))
	}

	sc, err := storage.NewClient(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sc.Close() })

	return gcpsdk.NewClient(sc)
}

type getClientFunc func(t *testing.T, bucket string) storagedriver.Client

var clientNames = map[int]string{
	0: "fake",
	1: "real",
}

func TestGcsClient_PutGetRoundTrip(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			data := []byte("hello gcs")
			err := client.PutObject(ctx, bucketName, "putGetRoundTrip", data)
			require.NoError(t, err)

			got, err := client.GetObject(ctx, bucketName, "putGetRoundTrip")
			require.NoError(t, err)
			assert.Equal(t, data, got)
		})
	}
}

func TestGcsClient_ObjectExists(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			exists, err := client.ObjectExists(ctx, bucketName, "objectExists-missing-key")
			require.NoError(t, err)
			assert.False(t, exists)

			err = client.PutObject(ctx, bucketName, "objectExists-present-key", []byte("data"))
			require.NoError(t, err)

			exists, err = client.ObjectExists(ctx, bucketName, "objectExists-present-key")
			require.NoError(t, err)
			assert.True(t, exists)
		})
	}
}

func TestGcsClient_ObjectExists_BucketNotFound(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			exists, err := client.ObjectExists(ctx, missingBucketName, "my/key")
			assert.NoError(t, err)
			assert.False(t, exists)
		})
	}
}

func TestGcsClient_GetObject_NotFound(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			_, err := client.GetObject(ctx, bucketName, "getObject-no-such-key")
			assert.Error(t, err)
			assert.ErrorIs(t, err, storage.ErrObjectNotExist)
			// check for an explicit message to show that error shows
			// proper diagnostic information
			assert.ErrorContains(t, err, "storage: object doesn't exist")
		})
	}
}

func TestGcsClient_GetObject_BucketNotFound(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			_, err := client.GetObject(ctx, missingBucketName, "my/key")
			t.Log(err)
			assert.Error(t, err)
			assert.ErrorIs(t, err, storage.ErrObjectNotExist)
			// check for an explicit message to show that error shows
			// proper diagnostic information. Currently, this will
			// always show "object doesn't exist" message even if the
			// bucket is not found.
			assert.ErrorContains(t, err, "storage: object doesn't exist")
		})
	}
}

func TestGcsClient_PutObject_BucketNotFound(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			err := client.PutObject(ctx, missingBucketName, "my/key", []byte("data"))
			assert.Error(t, err)
			// For some reason this API does wrap a google api error
			// and returns missing bucket information. But not the
			// fake.
			if clientNames[i] == "real" {
				assert.ErrorContains(t, err, "The specified bucket does not exist.")
			}
		})
	}
}

func TestGcsClient_LargeObject(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			ctx := context.Background()

			// 1 MiB of data.
			data := make([]byte, 1024*1024)
			for j := range data {
				data[j] = byte(j % 256)
			}

			err := client.PutObject(ctx, bucketName, "largeObject", data)
			require.NoError(t, err)

			got, err := client.GetObject(ctx, bucketName, "largeObject")
			require.NoError(t, err)
			assert.Equal(t, data, got)
		})
	}
}

func TestGcsClient_Describe_ReturnsNil(t *testing.T) {

	clientFns := []getClientFunc{newFakeGCS, newRealGCS}
	for i, clientFn := range clientFns {
		t.Run(clientNames[i], func(t *testing.T) {
			client := clientFn(t, bucketName)
			assert.Nil(t, client.Describe())
		})
	}
}
