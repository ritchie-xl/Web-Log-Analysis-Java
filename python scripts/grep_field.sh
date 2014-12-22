#!/bin/bash

# Find out

# grep: filters its input according to the give patterns and outpus only matching lines
# -E: turn on extended regular expressions
#-o: causes grep to only output the portion of the input that matches the pattern

#hadoop jar $STREAMING
# -D stream.non.zero.exit.is.failure=false
# -input data/heckle/
# -input data/jeckle/
# -output types
# -mapper "grep_field.sh type"
# -file grep_field.sh
# -reducer "uniq -c"

grep -Eo "\"$1\":[^,]+" | cut -d: -f2- | tr -d '" '