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
	"sync"
	"sync/atomic"
	"time"
)

var Verbose = 1
var refreshTime = time.Minute * 5

func Debug(level int, format string, args ...interface{}) {
	if level > Verbose {
		return
	}
	log.Printf(format, args...)
}

type COSClient struct {
	APIKey      string
	IAMEndpoint string
	ID          string

	Token        string
	Expires      time.Time
	RefreshMutex sync.Mutex

	Endpoints map[string]string // BucketName -> URL
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

func NewClient(apikey, id string) (*COSClient, error) {
	if apikey == "" {
		return nil, fmt.Errorf("Missing APIKey")
	}

	if id == "" {
		return nil, fmt.Errorf("Missing COS Instance ID")
	}

	client := &COSClient{
		APIKey:      apikey,
		IAMEndpoint: "https://iam.cloud.ibm.com/identity/token",
		ID:          id,

		Token:   "",
		Expires: time.Time{},
	}

	// if err := client.Refresh(); err != nil {
	// return nil, err
	// }

	return client, nil
}

func (client *COSClient) Refresh() error {
	client.RefreshMutex.Lock()
	defer client.RefreshMutex.Unlock()

	if time.Now().Add(refreshTime).Before(client.Expires) {
		return nil
	}

	log.Printf("Refreshing COS token")
	bodyStr := "apikey=" + url.PathEscape(client.APIKey) + "&" +
		"response_type=cloud_iam&" +
		"grant_type=urn:ibm:params:oauth:grant-type:apikey"

	req, err := http.NewRequest("POST", client.IAMEndpoint,
		strings.NewReader(bodyStr))
	if err != nil {
		return fmt.Errorf("Error creating HTTP client: %s", err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Close = true

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpClient := &http.Client{Transport: tr}
	res, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Error getting IAM token: %s", err)
	}

	defer httpClient.CloseIdleConnections()
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Error http response: %s", err)
	}

	data := struct {
		Access_token  string
		Expiration    int64
		expires_in    int
		Refresh_token string
		Scope         string
		Token_type    string
		// or...
		ErrorMessage string
	}{}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return fmt.Errorf("Error parsing response: %s\n%s",
			err, string(body))
	}

	if data.ErrorMessage != "" {
		return fmt.Errorf(data.ErrorMessage)
	}

	client.Token = data.Access_token
	client.Expires = time.Unix(data.Expiration, 0)

	return nil
}

func (client *COSClient) doHTTP(method string, path string, body []byte, num int, headers map[string]string) ([]byte, error) {

	// Refresh if needed
	client.Refresh()

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
		Debug(2, "SVC-ID: %s\n", client.ID[:15])
	}

	for k, v := range headers {
		req.Header.Add(k, v)
		Debug(2, "HEADER: %s: %s\n", k, v)
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

func (client *COSClient) CreateBucket(name, daType, reg string) error {
	//                   type       reg        scope      name   url
	//                 cross-region us         public     us-geo s3.us...
	endpoints, err := GetCOSEndpoints()
	if err != nil {
		return err
	}

	daTypes := endpoints.ServiceEndpoints[daType]
	if daTypes == nil {
		values := []string{}
		for key, _ := range endpoints.ServiceEndpoints {
			values = append(values, key)
		}
		return fmt.Errorf("Unknown type of region %q (can be: %s)", daType,
			strings.Join(values, ","))
	}

	region := daTypes[reg]
	if region == nil {
		values := []string{}
		for key, _ := range daTypes {
			values = append(values, key)
		}
		return fmt.Errorf("Unknown region %q (can be: %s)", reg,
			strings.Join(values, ","))
	}

	nameMap := region["public"]
	for _, endpoint := range nameMap {
		path := fmt.Sprintf("https://%s/%s", endpoint, name)

		_, err := client.doHTTP("PUT", path, nil, 2, nil)
		return err
	}

	return fmt.Errorf("Can't find endpoint for %s/%s region", daType, reg)
}

func (client *COSClient) GetBucketMetadata(name string) (*BucketMetadata, error) {
	// {"name":"customers","service_instance_id":"ad58e4cf-c3f4-49b8-b34a-70a15a416c58","time_created":"2020-04-26T13:36:44.663Z","time_updated":"2020-04-27T01:34:14.856Z","object_count":1847,"bytes_used":82860,"crn":"crn:v1:staging:public:cloud-object-storage:global:a/80368303fa866f52abd5c0e96e771db3:ad58e4cf-c3f4-49b8-b34a-70a15a416c58:bucket:customers","service_instance_crn":"crn:v1:staging:public:cloud-object-storage:global:a/80368303fa866f52abd5c0e96e771db3:ad58e4cf-c3f4-49b8-b34a-70a15a416c58::"}

	svcURL, err := client.GetEndpointForBucket(name)
	if err != nil {
		return nil, err
	}

	test := ""
	if strings.Index(svcURL, ".test.") > 0 {
		test = "test."
	}
	path := fmt.Sprintf("https://config.cloud-object-storage.%scloud.ibm.com/v1/b/%s", test, name)

	body, err := client.doHTTP("GET", path, nil, 1, nil)
	if err != nil {
		return nil, fmt.Errorf("GetBucketMetadata/GET(%s): %s", path, err)
	}

	res := BucketMetadata{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

type COSEndpoints struct {
	IdentityEndpoints struct {
		IAMToken  string `json:"iam-token"`
		IAMPolicy string `json:"iam-policy"`
	} `json:"identity-endpoints"`
	ServiceEndpoints map[string]map[string]map[string]map[string]string `json:"service-endpoints"`
	//                   type       reg        scope      name   url
	//                 cross-region us         public     us-geo s3.us...
	//                 regional     us-south   private    us-south s3...
	//                 single-site  hkg02      direct     hkg02  s3...
}

var Endpoints = (*COSEndpoints)(nil)

func GetCOSEndpoints() (*COSEndpoints, error) {
	Debug(2, "In GetCOSEndpoints\n")
	if Endpoints != nil {
		return Endpoints, nil
	}

	path := "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints"
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("Creating HTTP client: %s", err)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	Debug(2, "PATH: %s\n", path)
	Debug(2, "METHOD: GET\n")

	cli := &http.Client{Transport: tr}
	res, err := cli.Do(req)
	if err != nil {
		Debug(2, "ERR: %s\n", err)
		return nil, fmt.Errorf("%s", err)
	}

	defer res.Body.Close()
	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading endpoints: %s", err)
	}
	err = json.Unmarshal(buf, &Endpoints)
	if err != nil {
		err = fmt.Errorf("Error parsing endpoints: %s", err)
		Debug(2, "ERR: %s\n", err)
		return nil, err
	}
	Debug(2, ToJsonString(Endpoints))
	return Endpoints, nil
}

func ToJsonString(obj interface{}) string {
	buf, _ := json.MarshalIndent(obj, "", "  ")
	return string(buf)
}

func (client *COSClient) GetEndpointForBucket(name string) (string, error) {
	// cross:  ap-smart
	// cross:  us-standard
	// reg  :  eu-de-standard
	// reg  :  us-south-smart

	Debug(2, "Getting endpoints for bucket %q\n", name)
	if client.Endpoints != nil {
		if url, ok := client.Endpoints[name]; ok {
			Debug(2, "  -> %s\n", url)
			return url, nil
		}
	}

	endpoints, err := GetCOSEndpoints()
	if err != nil {
		return "", fmt.Errorf("GetEndpointsForBucket/GetCOSEndpoint: %s", err)
	}

	list, err := client.ListBuckets()
	if err != nil {
		return "", fmt.Errorf("GetEndpointsForBucket/ListBuckets: %s", err)
	}

	for _, bucket := range list.Buckets {
		Debug(2, "Compare: %q vs %q", name, bucket.Name)
		if bucket.Name != name {
			continue
		}
		Debug(2, "Found it")
		Debug(2, "Bucket info:\n%s\n", ToJsonString(bucket))

		// Found it
		loc := bucket.LocationConstraint
		//                   type       reg        scope      name   url
		//                 cross-region us         public     us-geo s3.us...
		//                 regional     us-south   private    us-south s3...
		//                 single-site  hkg02      direct     hkg02  s3...

		parts := strings.Split(loc, "-")
		daType := ""
		reg := ""
		if len(parts) == 2 {
			daType = "cross-region"
			reg = parts[0]

			Debug(2, "Reg: %s", reg)
			for siteName, _ := range endpoints.ServiceEndpoints["single-site"] {
				if siteName == reg {
					daType = "single-site"
					break
				}
			}
		} else if len(parts) == 3 {
			daType = "regional"
			reg = parts[0] + "-" + parts[1]
		} else {
			return "", fmt.Errorf("Can't split loc: %s", loc)
		}
		Debug(2, "type: %s", daType)
		Debug(2, "reg: %s", reg)

		names := endpoints.ServiceEndpoints[daType][reg]["public"]
		Debug(2, "Svc Endpoints: %v", names)
		for name, url := range names {
			url := "https://" + url

			if client.Endpoints == nil {
				client.Endpoints = map[string]string{}
			}
			client.Endpoints[name] = url
			return url, nil
		}

		// Debug(2, "Svc Endpoints: %#v", endpoints.ServiceEndpoints)
		err = fmt.Errorf("Can't find endpoint for bucket: %s", name)
		return "", err
	}

	err = fmt.Errorf("Can't find bucket: %s", name)
	Debug(2, "ERR: %s\n", err)
	return "", err
}

func (client *COSClient) ListBuckets() (*BucketList, error) {
	path := fmt.Sprintf("%s?extended", "https://s3.us.cloud-object-storage.appdomain.cloud")

	body, err := client.doHTTP("GET", path, nil, 2, nil)
	if err != nil {
		return nil, fmt.Errorf("ListBuckets/GET(%s): %s", path, err)
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
	svcURL, err := client.GetEndpointForBucket(name)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%s", svcURL, name)

	_, err = client.doHTTP("DELETE", path, nil, 1, nil)
	return err
}

func (client *COSClient) GetBucketLocation(name string) (string, error) {
	svcURL, err := client.GetEndpointForBucket(name)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("%s/%s?location", svcURL, name)

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
	path := fmt.Sprintf("%s/%s", "https://s3.us.cloud-object-storage.appdomain.cloud", name)
	_, err := client.doHTTP("HEAD", path, nil, 1, nil)
	return err == nil
}

func (client *COSClient) ListObjects(bucket string) (ObjectList, error) {
	// <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>dugs</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><Delimiter></Delimiter><IsTruncated>false</IsTruncated><Contents><Key>file2</Key><LastModified>2020-04-25T12:06:55.310Z</LastModified><ETag>&quot;5eb63bbbe01eeed093cb22bb8f5acdc3&quot;</ETag><Size>11</Size><Owner><ID>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</ID><DisplayName>ad58e4cf-c3f4-49b8-b34a-70a15a416c58</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>
	// GET /bucket

	contToken := ""
	res := ObjectList{}

	svcURL, err := client.GetEndpointForBucket(bucket)
	if err != nil {
		return nil, err
	}

	for {
		// path := fmt.Sprintf("%s/%s?list-type=2", svcURL, bucket)
		path := fmt.Sprintf("%s/%s?extended", svcURL, bucket)
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

	svcURL, err := client.GetEndpointForBucket(bucket)
	if err != nil {
		return fmt.Errorf("Getting getting endpoint(%s): %s", bucket, err)
	}

	path := fmt.Sprintf("%s/%s/%s", svcURL, bucket, name)

	_, err = client.doHTTP("PUT", path, data, 1, nil)
	if err != nil {
		err = fmt.Errorf("PUT error(%s): %s", path, err)
	}
	return err
}

func (client *COSClient) DeleteObject(bucket, name string) error {
	// DELETE /bucket/file

	svcURL, err := client.GetEndpointForBucket(bucket)
	if err != nil {
		return fmt.Errorf("Getting getting endpoint(%s): %s", bucket, err)
	}

	path := fmt.Sprintf("%s/%s/%s", svcURL, bucket, name)

	_, err = client.doHTTP("DELETE", path, nil, 1, nil)
	if err != nil {
		err = fmt.Errorf("DELETE error(%s): %s", path, err)
	}
	return err
}

func (client *COSClient) DeleteObjects(bucket string, names []string) error {
	// DELETE /bucket/file

	svcURL, err := client.GetEndpointForBucket(bucket)
	if err != nil {
		return fmt.Errorf("Getting getting endpoint(%s): %s", bucket, err)
	}

	path := fmt.Sprintf("%s/%s?delete", svcURL, bucket)

	body := "<Delete>"
	for _, name := range names {
		body += "<Object><Key>" + name + "</Key></Object>"
	}
	body += "</Delete>"

	sum := md5.Sum([]byte(body))

	headers := map[string]string{}
	headers["Content-MD5"] = base64.StdEncoding.EncodeToString(sum[:])

	_, err = client.doHTTP("POST", path, []byte(body), 1, headers)
	if err != nil {
		err = fmt.Errorf("DELETE/POST error(%s): %s", path, err)
	}
	return err
}

func (client *COSClient) DownloadObject(bucket, name string) ([]byte, error) {
	// GET /bucket/file

	svcURL, err := client.GetEndpointForBucket(bucket)
	if err != nil {
		return nil, fmt.Errorf("Getting getting endpoint(%s): %s", bucket, err)
	}

	path := fmt.Sprintf("%s/%s/%s", svcURL, bucket, name)

	data, err := client.doHTTP("GET", path, nil, 1, nil)
	return data, err
}

func (client *COSClient) CopyObject(srcBucket, srcName, tgtBucket, tgtName string) error {
	svcURL, err := client.GetEndpointForBucket(tgtBucket)
	if err != nil {
		return err
	}

	URL, _ := url.Parse(svcURL)
	URL.Host = tgtBucket + "." + URL.Host
	svcURL = URL.String()

	path := fmt.Sprintf("%s/%s", svcURL, tgtName)
	headers := map[string]string{
		"X-Amz-Copy-Source":       fmt.Sprintf("/%s/%s", srcBucket, srcName),
		"Ibm-Service-Instance-Id": client.ID,
	}

	_, err = client.doHTTP("PUT", path, nil, 1, headers)
	return err
}
