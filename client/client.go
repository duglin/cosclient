package cosclient

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

var verbose = 1

func Debug(level int, format string, args ...interface{}) {
	if level > verbose {
		return
	}
	log.Printf(format, args...)
}

type COSClient struct {
	Endpoint string
	Token    string
	ID       string
}

type BucketMetadata struct {
	Name                 string
	Service_Instance_ID  string
	Created              string `json:"time_updated"`
	Updated              string `json:"time_updated"`
	Objects              int    `json:"object_count"`
	Bytes                int    `json:"bytes_used"`
	CRN                  string
	Service_Instance_CRN string
}

type BucketListResponse struct {
	Owner struct {
		ID          string
		DisplayName string
	}
	Marker  string
	Buckets struct {
		Bucket []struct {
			Name               string
			CreationDate       string
			LocationConstraint string
		}
	}
}

type BucketList struct {
	Owner struct {
		ID          string
		DisplayName string
	}
	Buckets []struct {
		Name               string
		CreationDate       string
		LocationConstraint string
	}
}

type ObjectMetadata struct {
	Key          string
	LastModified string
	Size         int
}

type ObjectList []ObjectMetadata

type ObjectListResponse struct {
	Name                  string
	Prefix                string
	NextContinuationToken string
	KeyCount              int
	MaxKeys               int
	Delimiter             string
	IsTruncated           bool
	Contents              []struct {
		Key          string
		LastModified string
		ETag         string
		Size         int
		Owner        struct {
			ID          string
			DisplayName string
		}
		StorageClass string
	}
}

// https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-curl
// https://cloud.ibm.com/docs/services/cloud-object-storage?topic=cloud-object-storage-compatibility-api-bucket-operations#compatibility-api-new-bucket

func NewClient(apikey, iam_endpoint, endpoint, id string) (*COSClient, error) {
	iam_endpoint = strings.TrimRight(iam_endpoint, "/")
	endpoint = strings.TrimRight(endpoint, "/")

	bodyStr := "apikey=" + url.PathEscape(apikey) + "&" +
		"response_type=cloud_iam&" +
		"grant_type=urn:ibm:params:oauth:grant-type:apikey"

	req, err := http.NewRequest("POST", iam_endpoint, strings.NewReader(bodyStr))
	if err != nil {
		return nil, fmt.Errorf("Error creating HTTP client: %s", err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Close = true

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error getting IAM token: %s", err)
	}

	defer client.CloseIdleConnections()
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Error http response: %s", err)
	}

	data := map[string]interface{}{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, fmt.Errorf("Error parsing response: %s\n%s",
			err, string(body))
	}

	token, ok := data["access_token"].(string)
	if !ok {
		return nil, fmt.Errorf("Error parsing token: %q", data) //["access_token"])
	}

	return &COSClient{
		Endpoint: endpoint,
		Token:    token,
		ID:       id,
	}, nil
}

func (client *COSClient) doHTTP(method string, path string, body []byte, num int, headers map[string]string) ([]byte, error) {
	reader := bytes.NewReader(body)
	req, err := http.NewRequest(method, path, reader)
	if err != nil {
		return nil, fmt.Errorf("Creating HTTP client: %s", err)
	}

	Debug(2, "PATH: %s\n", path)
	Debug(2, "METHOD: %s\n", method)
	Debug(2, "BODY: %s\n", string(body))

	req.Header.Add("Authorization", "Bearer "+client.Token)
	Debug(2, "AUTH: %s\n", req.Header.Get("Authorization")[:15])
	req.Close = true

	if num > 1 {
		req.Header.Add("ibm-service-instance-id", client.ID)
		Debug(2, "SVC-ID: %s\n", req.Header.Get("ibm-service-instance-id")[:15])
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	cli := &http.Client{Transport: tr}
	res, err := cli.Do(req)
	if err != nil {
		Debug(2, "ERR: %s\n", err)
		return nil, fmt.Errorf("%s", err)
	}

	defer cli.CloseIdleConnections()
	defer res.Body.Close()
	body, err = ioutil.ReadAll(res.Body)
	if err != nil {
		Debug(2, "ERR: %s\n", err)
		return nil, fmt.Errorf("%s", err)
	}

	if res.StatusCode/100 != 2 {
		err = fmt.Errorf("%s", res.Status)
		if len(body) > 0 {
			err = fmt.Errorf("%s: %s", err, string(body))
		}
		Debug(2, "ERR: %s\n", err)
		return nil, err
	}
	return body, nil
}

func (client *COSClient) CreateBucket(name string) error {
	path := fmt.Sprintf("%s/%s", client.Endpoint, name)

	_, err := client.doHTTP("PUT", path, nil, 2, nil)
	return err
}

func (client *COSClient) GetBucketMetadata(name string) (*BucketMetadata, error) {
	// {"name":"customers","service_instance_id":"ad58e4cf-c3f4-49b8-b34a-70a15a416c58","time_created":"2020-04-26T13:36:44.663Z","time_updated":"2020-04-27T01:34:14.856Z","object_count":1847,"bytes_used":82860,"crn":"crn:v1:staging:public:cloud-object-storage:global:a/80368303fa866f52abd5c0e96e771db3:ad58e4cf-c3f4-49b8-b34a-70a15a416c58:bucket:customers","service_instance_crn":"crn:v1:staging:public:cloud-object-storage:global:a/80368303fa866f52abd5c0e96e771db3:ad58e4cf-c3f4-49b8-b34a-70a15a416c58::"}

	test := ""
	if strings.Index(client.Endpoint, ".test.") > 0 {
		test = "test."
	}
	path := fmt.Sprintf("https://config.cloud-object-storage.%scloud.ibm.com/v1/b/%s", test, name)

	body, err := client.doHTTP("GET", path, nil, 1, nil)
	if err != nil {
		return nil, err
	}

	res := BucketMetadata{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func (client *COSClient) ListBuckets() (*BucketList, error) {
	path := fmt.Sprintf("%s?extended", client.Endpoint)

	body, err := client.doHTTP("GET", path, nil, 2, nil)
	if err != nil {
		return nil, err
	}

	// <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</ID><DisplayName>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</DisplayName></Owner><Buckets><Bucket><Name>coligo-test</Name><CreationDate>2020-04-22T15:45:29.201Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>

	buckets := BucketListResponse{}

	err = xml.Unmarshal(body, &buckets)
	if err != nil {
		return nil, fmt.Errorf("Error parsing result: %s", err)
	}

	res := BucketList{
		Owner: buckets.Owner,
	}
	for _, bucket := range buckets.Buckets.Bucket {
		res.Buckets = append(res.Buckets, bucket)
	}

	return &res, nil
}

func (client *COSClient) DeleteBucket(name string) error {
	path := fmt.Sprintf("%s/%s", client.Endpoint, name)

	_, err := client.doHTTP("DELETE", path, nil, 1, nil)
	return err
}

func (client *COSClient) GetBucketLocation(name string) (string, error) {
	path := fmt.Sprintf("%s/%s?location", client.Endpoint, name)

	body, err := client.doHTTP("GET", path, nil, 1, nil)
	return string(body), err
}

func (client *COSClient) DeleteBucketContents(name string) error {
	count := int32(0)
	var resErr error

	list, err := client.ListObjects(name)
	if err != nil {
		return fmt.Errorf("Error getting bucket contents: %s", err)
	}

	if len(list) == 0 {
		return nil
	}

	start := 0
	size := len(list)
	end := 0
	for end != size {
		if start+1000 > size {
			end = size
		} else {
			end = start + 1000
		}

		objects := []string{}
		for _, object := range list[start:end] {
			objects = append(objects, object.Key)
		}
		start = end

		for atomic.LoadInt32(&count) >= 10 {
			time.Sleep(time.Second)
		}

		atomic.AddInt32(&count, 1)
		go func(objects []string, pErr *error) {
			defer atomic.AddInt32(&count, -1)

			fmt.Printf("deleting %d objects\n", len(objects))
			err := client.DeleteObjects(name, objects)
			if err != nil {
				*pErr = fmt.Errorf("Error deleting bucket contents: %s", resErr)
			}
		}(objects, &resErr)
	}

	for atomic.LoadInt32(&count) > 0 {
		time.Sleep(time.Second)
	}

	return resErr
}

func (client *COSClient) DeleteBucketAll(name string) error {
	err := client.DeleteBucketContents(name)
	if err != nil {
		return err
	}

	return client.DeleteBucket(name)
}

func (client *COSClient) BucketExists(name string) bool {
	// https://config.cloud-object-storage.cloud.ibm.com/v1/b/

	// path := "https://config.cloud-object-storage.cloud.ibm.com/v1/b/" + name
	path := fmt.Sprintf("%s/%s", client.Endpoint, name)
	_, err := client.doHTTP("HEAD", path, nil, 1, nil)
	return err == nil
}

func (client *COSClient) ListObjects(bucket string) (ObjectList, error) {
	// <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>dugs</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><Delimiter></Delimiter><IsTruncated>false</IsTruncated><Contents><Key>file2</Key><LastModified>2020-04-25T12:06:55.310Z</LastModified><ETag>&quot;5eb63bbbe01eeed093cb22bb8f5acdc3&quot;</ETag><Size>11</Size><Owner><ID>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</ID><DisplayName>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>
	// GET /bucket

	contToken := ""
	res := ObjectList{}

	for {
		// path := fmt.Sprintf("%s/%s?list-type=2", client.Endpoint, bucket)
		path := fmt.Sprintf("%s/%s?extended", client.Endpoint, bucket)
		if contToken != "" {
			path += "&continuation-token=" + contToken
			contToken = ""
		}
		body, err := client.doHTTP("GET", path, nil, 2, nil)
		if err != nil {
			return nil, err
		}

		objects := ObjectListResponse{}
		err = xml.Unmarshal(body, &objects)
		if err != nil {
			return nil, fmt.Errorf("Error parsing result: %s", err)
		}

		for _, obj := range objects.Contents {
			newObj := ObjectMetadata{
				Key:          obj.Key,
				LastModified: obj.LastModified,
				Size:         obj.Size,
			}
			res = append(res, newObj)
		}

		contToken = objects.NextContinuationToken
		if contToken == "" {
			break
		}
	}

	return res, nil
}

func (client *COSClient) UploadObject(bucket, name string, data []byte) error {
	// PUT /bucket/file
	path := fmt.Sprintf("%s/%s/%s", client.Endpoint, bucket, name)

	_, err := client.doHTTP("PUT", path, data, 1, nil)
	return err
}

func (client *COSClient) DeleteObject(bucket, name string) error {
	// DELETE /bucket/file
	path := fmt.Sprintf("%s/%s/%s", client.Endpoint, bucket, name)

	_, err := client.doHTTP("DELETE", path, nil, 1, nil)
	return err
}

func (client *COSClient) DeleteObjects(bucket string, names []string) error {
	// DELETE /bucket/file
	path := fmt.Sprintf("%s/%s?delete", client.Endpoint, bucket)

	body := "<Delete>"
	for _, name := range names {
		body += "<Object><Key>" + name + "</Key></Object>"
	}
	body += "</Delete>"

	sum := md5.Sum([]byte(body))

	headers := map[string]string{}
	headers["Content-MD5"] = base64.StdEncoding.EncodeToString(sum[:])

	_, err := client.doHTTP("POST", path, []byte(body), 1, headers)
	return err
}

func (client *COSClient) DownloadObject(bucket, name string) ([]byte, error) {
	// GET /bucket/file
	path := fmt.Sprintf("%s/%s/%s", client.Endpoint, bucket, name)

	data, err := client.doHTTP("GET", path, nil, 1, nil)
	return data, err
}
