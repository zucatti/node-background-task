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

var redis = require("./data_store"),
  util = require('util'),
  utils = require('./utils'),
  EventEmitter = require('events').EventEmitter;


var Blacklist = exports.Blacklist = function(options, callback){     // Optional callback for auth operation test case
    EventEmitter.call(this);

    if (!options){
        throw new Error("I need a task key!");
    }

    this.taskKey = options.taskKey;

    this.failureInterval = options.failureInterval || 1;
    this.blacklistThreshold = options.blacklistThreshold || 10;
    this.globalBlacklistTimeout = options.globalBlacklistTimeout || 3600;

    this.logBlacklist = options.logBlacklist || false; // NB: This won't work if you want the default to be true

    this.keyPrefix = "blacklist:";
    this.globalBlacklistKeyPrefix = this.keyPrefix + "globalBlacklist:";
    this.blacklistLogKeyPrefix = this.keyPrefix + "logs:";

    this.blacklistClient = redis.initialize(options, callback);

    this.blacklistClient.on('error', utils.wrapError(this));
};

// Inherit EventEmitter's methods
util.inherits(Blacklist, EventEmitter);

Blacklist.prototype.blacklistStatus = function(task, callback){
    var taskKey = task && task[this.taskKey],
      key,
      that = this;

    if (!callback){
        callback = function(){};
    }

    if (!taskKey){
        callback(false, "No task key, can't check blacklist.");
    } else {
        key = that.globalBlacklistKeyPrefix + taskKey;
        that.blacklistClient.blacklistStore.check(key, function(error, reply){
            if (reply){
                // We're blacklisted
              callback(reply.status, reply.timeRemaining, reply.result);

            } else {
                callback(false, -1, "");
            }
        });
    }
};

Blacklist.prototype.addFailure = function(taskKey, reason, callback){
    var countKey, blacklistKey, that = this;

    if (!callback){
        callback = function(){};
    }

    if (!reason){
        callback(new Error("Must supply a reason for the failure"));
        return;
    }

    if (!taskKey){
        callback(new Error("Invalid task, not running."));
    } else {
        countKey = that.keyPrefix + taskKey + ":count";
        blacklistKey = that.globalBlacklistKeyPrefix + taskKey;

        that.blacklistClient.blacklistCounterStore.increment(countKey, that.failureInterval, "1", function(error, reply){

            if (error) {
              callback(error);
            } else {
              if (reply > that.blacklistThreshold) {
                that.blacklistClient.blacklistStore.add(blacklistKey, that.globalBlacklistTimeout, reason, function(e, r) {
                  var logKey, d;
                  if (!e) {
                    if (that.logBlacklist) {
                      d = new Date();
                      logKey = that.blacklistLogKeyPrefix + taskKey;
                      that.blacklistClient.blacklistStore.log(logKey, d + "|" + reason, function(){});
                    }
                    callback("Blacklisted");
                  } else {
                    callback(error);
                  }
                });
              } else {
                callback("OK");
              }
            }
        });
    }
  };

Blacklist.prototype.shutdown = function(){
    this.blacklistClient.shutdown();
};


