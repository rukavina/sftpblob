package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"gocloud.dev/blob"

	// Import the blob packages we want to be able to open.
	_ "github.com/rukavina/sftpblob"
	_ "gocloud.dev/blob/fileblob"
)

func main() {
	// Define our input.
	if len(os.Args) != 3 {
		log.Fatal("usage: upload BUCKET_URL FILE")
	}
	bucketURL := os.Args[1]
	file := os.Args[2]

	ctx := context.Background()
	// Open a connection to the bucket.
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Fatalf("Failed to setup bucket: %s", err)
	}
	defer b.Close()

	if file == "list" {
		list(ctx, b, "", "  ")
		return
	}

	// Prepare the file for upload.
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}

	w, err := b.NewWriter(ctx, file, nil)
	if err != nil {
		log.Fatalf("Failed to obtain writer: %s", err)
	}
	_, err = w.Write(data)
	if err != nil {
		log.Fatalf("Failed to write to bucket: %s", err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to close: %s", err)
	}
}

func list(ctx context.Context, b *blob.Bucket, prefix, indent string) {
	iter := b.List(&blob.ListOptions{
		Delimiter: "/",
		Prefix:    prefix,
	})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s%s\n", indent, obj.Key)
		if obj.IsDir {
			list(ctx, b, obj.Key, indent+"  ")
		}
	}
}
