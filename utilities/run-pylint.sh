#!/bin/bash

pylint \
    --verbose \
    --jobs 0 \
    --reports y \
    --output-format text \
    --disable=too-many-locals \
    --disable=line-too-long \
    --disable=too-many-lines \
    --disable=too-many-return-statements \
    --disable=too-many-branches \
    --disable=too-many-statements \
    --disable=too-many-branches \
    --disable=too-many-nested-blocks \
    --disable=no-else-return \
    --disable=too-many-arguments \
    --disable=unused-argument \
    --disable=invalid-name \
    --disable=too-many-instance-attributes \
    --disable=consider-using-with \
    --disable=too-many-public-methods \
    --disable=too-many-positional-arguments \
    --disable=protected-access \
    "$@"
