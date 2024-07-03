package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"gocloud.dev/blob"

	// Import the blob packages we want to be able to open.
	_ "github.com/rukavina/sftpblob"
	_ "gocloud.dev/blob/fileblob"
)

func main() {
	// Define our input.
	if len(os.Args) != 4 {
		log.Fatal("usage: ./uploader BUCKET_URL COMMAND PARAM, where COMMAND can be: list|upload|mkdir")
	}
	bucketURL := os.Args[1]
	command := os.Args[2]
	param := os.Args[3]

	ctx := context.Background()
	// Open a connection to the bucket.
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Fatalf("Failed to setup bucket: %s", err)
	}
	defer b.Close()

	switch command {
	case "list":
		list(ctx, b, param, "  ")
	case "upload":
		upload(ctx, b, param)
	case "mkdir":
		mkdir(ctx, b, param)
	case "remove":
		remove(ctx, b, param)
	}

}

func upload(ctx context.Context, b *blob.Bucket, file string) {
	data, err := os.ReadFile(file)
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
		fmt.Printf("%s%s (%d B)\n", indent, obj.Key, obj.Size)
		//if dir, make new recursive call to the same func
		if obj.IsDir {
			list(ctx, b, obj.Key, indent+"  ")
		}
	}
}

func mkdir(ctx context.Context, b *blob.Bucket, dir string) {
	//we need to fake an empty new file inside of the new dir
	file := dir + "/" + ".newdir"
	w, err := b.NewWriter(ctx, file, nil)
	if err != nil {
		log.Fatalf("Failed to obtain writer: %s", err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to close: %s", err)
	}
}

func remove(ctx context.Context, b *blob.Bucket, file string) {
	err := b.Delete(ctx, file)
	if err != nil {
		log.Fatalf("Failed to delete %q: %v", file, err)
	}
}
