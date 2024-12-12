#!/bin/sh

go \
	build \
	-v \
	-tags fast_count \
	./...
