#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
cd "${SCRIPT_DIR}"

FORMAT_FILES_IN_DIRECTORIES="../src/ ../include/"

clang-format --version > /dev/null || exit 1

if [ "$1" = "check" ]; then # Check clang-format has been applied!
  find ${FORMAT_FILES_IN_DIRECTORIES} \
    -regex '.*\.\(cpp\|hpp\|cc\|cxx\)' \
    -exec clang-format -style=file -output-replacements-xml -i {} \; |
    grep -c "<replacement " >/dev/null

  if [ $? -ne 1 ]; then
    echo "Format check: Failed!"
    echo " -- Files do not match clang-format. Run bin/format.sh before adding files to git!"
    exit 1
  else
    echo "Format check: Passed!"
  fi

else # Apply clang-format to all files

  find ${FORMAT_FILES_IN_DIRECTORIES} \
    -regex '.*\.\(c\|h\|cpp\|hpp\|cc\|cxx\)' \
    -exec clang-format -style=file -i {} \;
fi
