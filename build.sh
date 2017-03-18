#!/usr/bin/env bash
TAG=$(git describe --tags --abbrev=0)
fileb0x templates/b0x.yaml
GOOS="linux" GOARCH="amd64" go build -o dist/dbsample cli/main.go
tar -cvzf dist/dbsample-${TAG}-linux-amd64.tar.gz -C dist dbsample

GOOS="linux" GOARCH="386" go build -o dist/dbsample cli/main.go
tar -cvzf dist/dbsample-${TAG}-linux-386.tar.gz -C dist dbsample
