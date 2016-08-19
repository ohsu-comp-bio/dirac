#!/usr/bin/env bash

protoc -I=schema/ohsu/schema/ --python_out=./ schema/ohsu/schema/core.proto
