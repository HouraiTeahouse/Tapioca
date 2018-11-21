#!/bin/sh
set -e

root_dir=$(pwd)
build_dir=$root_dir/bin
os=('windows' 'darwin' 'linux')
arch=('amd64' '386')

declare -a suffix
suffix[windows]='.exe'
suffix[linux]=''
suffix[darwin]=''

function build_binary() {
  cd $1
  binary_name=$(basename $1)
  output_base=$(realpath "${build_dir}/${binary_name}")
  for operating_system in "${os[@]}"; do
    for architecture in "${arch[@]}"; do
      output=${output_base}-${operating_system}-${architecture}${suffix["${operating_system}"]}
      CGO_ENABLED=0 GOOS=$operating_system GOARCH=$architecture go build -ldflags='-s -w' -o $output
    done
  done
}

set -o xtrace
mkdir -p $build_dir

for binary in ./cmd/*/; do
  build_binary $binary
done
