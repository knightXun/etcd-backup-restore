package snapstore

import (
	"context"
	"fmt"
	"math/rand"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
	"github.com/sirupsen/logrus"
)

//// COSBucket is an interface for oss.Bucket used in snapstore
//type COSBucket interface {
//	GetObject(objectKey string, options ...cos.Option) (io.ReadCloser, error)
//	InitiateMultipartUpload(objectKey string, options ...oss.Option) (oss.InitiateMultipartUploadResult, error)
//	CompleteMultipartUpload(imur cos.InitiateMultipartUploadResult, parts []oss.UploadPart, options ...oss.Option) (oss.CompleteMultipartUploadResult, error)
//	ListObjects(options ...cos.Option) (cos.ListObjectsResult, error)
//	DeleteObject(objectKey string, options ...oss.Option) error
//	UploadPart(imur oss.InitiateMultipartUploadResult, reader io.Reader, partSize int64, partNumber int, options ...oss.Option) (oss.UploadPart, error)
//	AbortMultipartUpload(imur oss.InitiateMultipartUploadResult, options ...oss.Option) error
//}

const (
	cosEndPoint           = "TENCENTCLOUD_ENDPOINT"
	cosAccessKeyID           = "TENCENTCLOUD_ACCESS_KEY_ID"
	cosAccessKeySecret       = "TENCENTCLOUD_ACCESS_KEY_SECRET"
)

// COSSnapStore is snapstore with tenc COS object store as backend
type COSSnapStore struct {
	prefix                  string
	client                  *cos.Client
	multiPart               sync.Mutex
	maxParallelChunkUploads int
	tempDir                 string
	bucket                  string
	ao                      authOptions
}

// NewCOSSnapStore create new COSSnapStore from shared configuration with specified bucket
func NewCOSSnapStore(bucket, prefix, tempDir string, maxParallelChunkUploads int) (*COSSnapStore, error) {
	ao, err := authCOSOptionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newCOSFromAuthOpt(bucket, prefix, tempDir, maxParallelChunkUploads, ao)
}

func newCOSFromAuthOpt(bucket, prefix, tempDir string, maxParallelChunkUploads int, ao authOptions) (*COSSnapStore, error) {
	u, _ := url.Parse("https://" + bucket + ".cos." + ao.endpoint +  ".myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Timeout: 100 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  ao.accessID,
			SecretKey: ao.accessKey,
		},
	})

	return NewCOSFromBucket(prefix, tempDir, maxParallelChunkUploads, client, ao, bucket), nil
}

// NewCOSFromBucket will create the new COS snapstore object from COS bucket
func NewCOSFromBucket(prefix, tempDir string, maxParallelChunkUploads int, client *cos.Client, ao authOptions, bucket string) *COSSnapStore {
	return &COSSnapStore{
		prefix:                  prefix,
		client:                  client,
		maxParallelChunkUploads: maxParallelChunkUploads,
		tempDir:                 tempDir,
		ao:                      ao,
		bucket: bucket,
	}
}

// Fetch should open reader for the snapshot file from store
func (s *COSSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	resp, err := s.client.Object.Get(context.Background(), path.Join(s.prefix, snap.SnapDir, snap.SnapName), nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func initUpload(c *cos.Client, name string) (*cos.InitiateMultipartUploadResult, error) {
	v, _, err := c.Object.InitiateMultipartUpload(context.Background(), name, nil)
	return v, err
}

func uploadPart(c *cos.Client, name string, uploadID string, blockSize, n int) (string, error) {

	b := make([]byte, blockSize)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	s := fmt.Sprintf("%X", b)
	f := strings.NewReader(s)

	resp, err := c.Object.UploadPart(
		context.Background(), name, uploadID, n, f, nil,
	)
	if err != nil {
		return "", err
	}
	fmt.Printf("%s\n", resp.Status)
	return resp.Header.Get("Etag"), err
}

// Save will write the snapshot to store
func (s *COSSnapStore) Save(snap Snapshot, rc io.ReadCloser) error {
	tmpfile, err := ioutil.TempFile(s.tempDir, tmpBackupFilePrefix)
	if err != nil {
		rc.Close()
		return fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

	size, err := io.Copy(tmpfile, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}
	_, err = tmpfile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var (
		chunkSize  = int64(math.Max(float64(minChunkSize), float64(size/ossNoOfChunk)))
		noOfChunks = size / chunkSize
	)
	if size%chunkSize != 0 {
		noOfChunks++
	}

	up, err := initUpload(s.client, path.Join(s.prefix, snap.SnapDir, snap.SnapName))
	if err != nil {
		return err
	}

	uploadID := up.UploadID
	blockSize := 1024 * 1024 * 1

	opt := &cos.CompleteMultipartUploadOptions{}
	for i := 1; i < 100; i++ {
		etag, err := uploadPart(s.client, path.Join(s.prefix, snap.SnapDir, snap.SnapName), uploadID, blockSize, i)

		if err != nil {
			return err
		}
		opt.Parts = append(opt.Parts, cos.Object{
			PartNumber: i, ETag: etag},
		)
	}

	//u, _ := url.Parse("https://" + s.bucket + ".cos." + s.ao.endpoint +  ".myqcloud.com")
	//b := &cos.BaseURL{BucketURL: u}
	//c := cos.NewClient(b, &http.Client{
	//	Transport: &cos.AuthorizationTransport{
	//		SecretID:  os.Getenv("COS_SECRETID"),
	//		SecretKey: os.Getenv("COS_SECRETKEY"),
	//		Transport: &debug.DebugRequestTransport{
	//			RequestHeader:  true,
	//			RequestBody:    true,
	//			ResponseHeader: true,
	//			ResponseBody:   true,
	//		},
	//	},
	//})

	_, _, err = s.client.Object.CompleteMultipartUpload(
		context.Background(), path.Join(s.prefix, snap.SnapDir, snap.SnapName), uploadID, opt,
	)

	if err != nil {
		logrus.Infof("Aborting the multipart upload with upload ID : %s",)
		return err
	}

	return nil
}

//func (s *COSSnapStore) partUploader(wg *sync.WaitGroup, imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, chunkUploadCh <-chan chunk, stopCh <-chan struct{}, errCh chan<- chunkUploadResult) {
//	defer wg.Done()
//	for {
//		select {
//		case <-stopCh:
//			return
//		case chunk, ok := <-chunkUploadCh:
//			if !ok {
//				return
//			}
//			logrus.Infof("Uploading chunk with id: %d, offset: %d, size: %d", chunk.id, chunk.offset, chunk.size)
//			err := s.uploadPart(imur, file, completedParts, chunk.offset, chunk.size, chunk.id)
//			errCh <- chunkUploadResult{
//				err:   err,
//				chunk: &chunk,
//			}
//		}
//	}
//}
//
//func (s *COSSnapStore) uploadPart(imur oss.InitiateMultipartUploadResult, file *os.File, completedParts []oss.UploadPart, offset, chunkSize int64, number int) error {
//	fd := io.NewSectionReader(file, offset, chunkSize)
//	part, err := s.bucket.UploadPart(imur, fd, chunkSize, number)
//
//	if err == nil {
//		completedParts[number-1] = part
//	}
//	return err
//}

// List will list the snapshots from store
func (s *COSSnapStore) List() (SnapList, error) {
	var snapList SnapList

	marker := ""
	for {
		opt := &cos.BucketGetOptions{
			Prefix:  marker,
		}
		v, _, err := s.client.Bucket.Get(context.Background(), opt)
		if err != nil {
			return nil, err
		}
		for _, object := range v.Contents {
			snap, err := ParseSnapshot(object.Key[len(s.prefix)+1:])
			if err != nil {
				// Warning
				logrus.Warnf("Invalid snapshot found. Ignoring it: %s", object.Key)
			} else {
				snapList = append(snapList, snap)
			}
		}
		if v.IsTruncated {
			marker = v.NextMarker
		} else {
			break
		}
	}
	sort.Sort(snapList)

	return snapList, nil
}

// Delete should delete the snapshot file from store
func (s *COSSnapStore) Delete(snap Snapshot) error {
	_, err := s.client.Object.Delete(context.Background(), path.Join(s.prefix, snap.SnapDir, snap.SnapName),nil)
	return err
}

func authCOSOptionsFromEnv() (authOptions, error) {
	endpoint, err := GetEnvVarOrError(cosEndPoint)
	if err != nil {
		return authOptions{}, err
	}
	accessID, err := GetEnvVarOrError(cosAccessKeyID)
	if err != nil {
		return authOptions{}, err
	}
	accessKey, err := GetEnvVarOrError(cosAccessKeySecret)
	if err != nil {
		return authOptions{}, err
	}

	ao := authOptions{
		endpoint:  endpoint,
		accessID:  accessID,
		accessKey: accessKey,
	}

	return ao, nil
}
