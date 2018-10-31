#!/bin/sh
set -e

PROTOBUF_VERSION=protobuf-2.4.1

# check to see if protobuf folder is empty
if [ ! -d "$HOME/protobuf/lib" ]; then
  wget https://protobuf.googlecode.com/files/$PROTOBUF_VERSION.tar.gz
  tar -xzvf $PROTOBUF_VERSION.tar.gz
  cd $PROTOBUF_VERSION && ./configure --prefix=$HOME/protobuf && make && make install
else
  echo "Using cached directory."
fi
