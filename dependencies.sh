#!/bin/bash

echo "Direct dependencies"
sbt 'show all-dependencies' | \
  gawk 'match($0, /List\((.*)\)/, a) {print a[1]}' | \
  tr -d ' ' | tr ',' '\n' | sort -t ':' | \
  tr ':' '\t' | expand -t 30

echo -e "\nAll dependencies, including transitive dependencies"
sbt 'show managed-classpath' | tr -d ' ' | tr ',' '\n' | \
  gawk 'match($0, /Attributed\((.*)\)/, a) {print a[1]}' | \
  tr -d '()' | sed "s^$HOME/.ivy2/cache/^^g" | sed "s^/jars^^" | \
  gawk -F / '{print $1, $3}' | sort | tr ' ' '\t' | expand -t 30
