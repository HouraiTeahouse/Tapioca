#!/bin/sh
set -e

# TODO(james7132): Have this work on OSX

PROTO_VERSION=$1
PROTO_DIR="$HOME/protoc"
ZIP_FILE="protoc-$PROTO_VERSION-linux-x86_64.zip"
DOWNLOAD_URL="https://github.com/protocolbuffers/protobuf/releases/download/v$PROTO_VERSION/$ZIP_FILE"

# check to see if protobuf folder is empty
if [ ! -d "$PROTO_DIR/bin" ]; then
  wget $DOWNLOAD_URL
  unzip $ZIP_FILE -d $PROTO_DIR
else
  echo "Using cached directory."
fi
