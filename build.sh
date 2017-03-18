#!/usr/bin/env bash
fileb0x templates/b0x.yaml
go build -o dist/dbsample cli/main.go

TAG=$(git describe --exact-match --tags $(git log -n1 --pretty='%h'))
tar -cvzf dist/dbsample-${TAG}-linux-amd64.tar.gz -C dist dbsample
