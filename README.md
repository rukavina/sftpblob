# gocloud SFTP Blob

## Overview
Attempt to develop blob support for SFTP protocol for gocloud framework.
It registers URL scheme `sftp://`. Bucket urls are regular sftp urls.

## Install

```bash
go get github.com/rukavina/sftpblob
```

## Example

As an usage example please check the code in `cmd/commander/main.go` and build it:

```bash
cd cmd/commander
go build
```

## Test commands

```bash
cd cmd/commander

#make sure that dir `/home/username/go-cloud/` exists

#upload via sftp
./commander sftp://username:password@127.0.0.1:22/home/username/go-cloud/ upload gopher.png

#list via sftp - make sure dir `test1` exists with some dummy content
./commander sftp://username:password@127.0.0.1:22/home/username/go-cloud/ list test1

#make dir
./commander sftp://username:password@127.0.0.1:22/home/username/go-cloud/ mkdir testnew

#delete (empty only!)
./commander sftp://username:password@127.0.0.1:22/home/username/go-cloud/ remove gopher.png
```