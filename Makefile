# Makefile for easy testing

test: all

all: test-background_task \
	test-blacklist \
	test-limits \
	test-messaging \
 	test-utils \
	test-data_store

test-background_task:
	@./node_modules/.bin/mocha test/test-background_task.js

test-blacklist:
	@./node_modules/.bin/mocha test/test-blacklist.js

test-limits:
	@./node_modules/.bin/mocha test/test-limits.js

test-messaging:
	@./node_modules/.bin/mocha test/test-messaging.js

test-utils:
	@./node_modules/.bin/mocha test/test-utils.js

test-data_store:
	@./node_modules/.bin/mocha test/test-data_store.js

.PHONY: test
