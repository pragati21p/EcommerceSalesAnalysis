#!/bin/bash

SUBDIR=$1
if [[ -z "$SUBDIR" ]]
then
    echo 'A subdirectory name must be passed as parameter'
    exit 1
fi

echo "Deleting /Shared/$SUBDIR"
databricks workspace delete --recursive /Shared/$SUBDIR

echo "Importing ../workspace /Shared/$SUBDIR/"
databricks workspace import_dir ../workspace /Shared/$SUBDIR/
