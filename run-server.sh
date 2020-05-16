#!/bin/bash

export JAVA_PROGRAM_ARGS=`echo "$@"`
mvn exec:java -Dexec.args="$JAVA_PROGRAM_ARGS"