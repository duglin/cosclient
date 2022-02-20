package main

import (
	"fmt"
	"os"

	// cosclient "github.ibm.com/coligo/demos/cosclient/client"
	cosclient "github.com/duglin/cosclient/client"
)

func main() {
	apikey := "..."
	// iam_endpoint := "https://iam.test.cloud.ibm.com/identity/token"
	// endpoint := "https://s3.us-west.cloud-object-storage.test.appdomain.cloud"
	res_id := "crn:v1:bluemix:public:cloud-object-storage:global:a/7f9dc5344476457f2c0f53244a246d44:c7f1202e-d7a3-4b92-8ce4-db0b3b372915::"

	cos, err := cosclient.NewClient(apikey, res_id)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	/*
		err = cos.DeleteBucketContents("records")
		if err != nil {
			fmt.Printf("err; %s\n", err)
			os.Exit(1)
		}
	*/

	/*
		md, err := cos.GetBucketMetadata("customers")
		fmt.Printf("%s, %d\n", md.Name, md.Objects)
		md, err = cos.GetBucketMetadata("records")
		fmt.Printf("%s, %d\n", md.Name, md.Objects)

		items, _ := cos.ListObjects("customers")
		fmt.Printf("Customers Size: %d\n", len(items))

		items, _ = cos.ListObjects("records")
		fmt.Printf("Records Size: %d\n", len(items))
		os.Exit(0)
	*/

	/*
			if cos.BucketExists("dugs") {
				fmt.Printf("Exists\n")
				os.Exit(1)
			}

		if err = cos.CreateBucket("dugs"); err != nil {
			fmt.Printf("Create: %s\n", err)
		}

		buckets, err := cos.ListBuckets()
		if err != nil {
			fmt.Printf("List: %s\n", err)
			os.Exit(1)
		}
		for _, bucket := range buckets.Buckets {
			fmt.Printf("  bucket: %s\n", bucket.Name)
		}

		cos.UploadObject("dugs", "file2", []byte("hello world"))

		objects, _ := cos.ListObjects("dugs")
		for _, object := range objects {
			fmt.Printf("Deleting: %s\n", object.Key)
			cos.DeleteObject("dugs", object.Key)
		}
	*/

	/*
		if err = cos.DeleteBucket("dugs"); err != nil {
			fmt.Printf("Delete: %s\n", err)
		}
	*/
	buckets, err := cos.ListBuckets()
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("buckets: %v\n", buckets.Buckets)

	err = cos.CopyObject("dug-fra", "2017-10-17_12-10-01_MHB_0911.jpg", "dug-par01", "2017-10-17_12-10-01_MHB_0911.jpg")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}
