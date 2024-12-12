#!/bin/sh

#pushd .
#cd sample.d/input.d
#python3 sample.py
#popd

#export ENV_SQLITE_DB_FILENAME=./sample.d/input.d/sample.sqlite.db
#export ENV_SCHEMA_FILENAME=./sample.d/input.d/sample.avsc
#echo '
#	SELECT * FROM sample_table1
#' |
#  sqlite2avro \
#  > ./sample.d/input.d/sample.avro

export ENV_SCHEMA_FILENAME=./sample.d/output.d/sample.avsc

export ENV_BLOB_KEY=data
export ENV_BLOB_ID_KEY=pid
export ENV_BLOB_EXT=txt.gz
export ENV_BLOB_DIRNAME=./sample.d/output.d/files.d
export ENV_COMPRESS_TYPE=gzip
export ENV_COMPRESS_LEVEL=1

cat ./sample.d/input.d/sample.avro |
	./avro2blob2ext |
	wc
