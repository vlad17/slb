#!/bin/bash
# cleans stdin for a wordcount, lower-casing all words and getting rid of
# all non-alphanumeric characters or dashes
# preserves newline structure
# all text book files are already cleaned this way

tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]- \n' ' '
