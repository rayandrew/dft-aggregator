#! /bin/bash

clang_format_exe="clang-format"
if [ $# -ge 1 ]; then
    clang_format_exe="$1"
fi

SUPPORTED_CLANG_FORMAT_VERSION="19.1.7"

if [ command -v $clang_format_exe >/dev/null 2>&1 ]; then
    echo "You must have 'clang-format' in PATH to use 'autoformat.sh'"
    exit 1
fi

clang_format_version_str=$($clang_format_exe --version)
clang_format_version=$(echo "$clang_format_version_str" | grep -oP 'clang-format version \K\d+(\.\d+)+')

if [ "$clang_format_version" != "$SUPPORTED_CLANG_FORMAT_VERSION" ]; then
    echo "WARNING: the .clang-format file in this repo is designed for version 18.1.8."
    echo "         You are running with clang-format v$clang_format_version."
    echo "         The resulting formatting is highly likely to be incorrect."
fi

if [ command -v find >/dev/null 2>&1 ]; then
    echo "You must have 'find' in PATH to use 'autoformat.sh'"
    exit 1
fi

if [ command -v dirname >/dev/null 2>&1 ]; then
    echo "You must have 'dirname' in PATH to use 'autoformat.sh'"
    exit 1
fi

if [ command -v xargs >/dev/null 2>&1 ]; then
    echo "You must have 'dirname' in PATH to use 'autoformat.sh'"
    exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

curr_dir=$(pwd)

cd $SCRIPT_DIR
cd ..
cd ..

for dir in src; do
  echo "Formatting C/C++ code in '$dir'"
  find $dir \( -name "*.c" -o -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \) -print0 | xargs -0 -P "$(nproc)" $clang_format_exe -i
done

cd $curr_dir
