// Copyright 2013 Kinvey, Inc
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

"use strict";

var redis = require("./data_store")
  , util = require('util')
  , utils = require('./utils')
  , async = require('async')
  , EventEmitter = require('events').EventEmitter
  , os = require('os');


var TaskLimit = exports.TaskLimit = function(options, callback){    // optional callback for tests
    EventEmitter.call(this);

    if (!options){
        throw new Error("I need a task key!");
    }

    this.taskKey = options.taskKey;
    this.keyPrefix = "taskKey:";
    this.maxTasksPerKey = options.maxTasksPerKey||10;
    this.taskLimitClient = redis.initialize(options, callback);

    this.taskLimitClient.on('error', utils.wrapError(this));
};

// Inherit EventEmitter's methods
util.inherits(TaskLimit, EventEmitter);

TaskLimit.prototype.startTask = function(task, callback){
    var value = task && task[this.taskKey]
      , key, that = this, hostname = os.hostname();

    if (!callback){
        callback = function(){};
    }

    if (!value){
        callback(new Error("Invalid task, not running."));
    } else {
        key = that.keyPrefix + value;

        that.taskLimitClient.limitStore.increment(key, JSON.stringify({date: Date(), host: hostname}), that.maxTasksPerKey, function(err, result){
            if (err){

                callback(err);
            } else {

                callback(result);
            }
        });
    }
};

TaskLimit.prototype.stopTask = function(task){
    var value = task && task[this.taskKey]
      , key, that = this;

    if (!value){
        throw new Error("Invalid task, can't stop.");
    } else {
        key = that.keyPrefix + value;
        that.taskLimitClient.limitStore.decrement(key, function() {});
    }
};

TaskLimit.prototype.shutdown = function(){
    var that = this;
    that.taskLimitClient.shutdown();
};

// TODO:  Redo cleanup function; rethink.

TaskLimit.prototype.cleanupTasks = function(callback) {
  var prefix, that = this
    , hostname = os.hostname(), currentKey, currentValue;

  prefix = that.keyPrefix + "*";

  that.taskLimitClient.limitStore.cleanup(prefix, hostname, function(err, result) {
    utils.processResult(err, result, callback);
  });
};
