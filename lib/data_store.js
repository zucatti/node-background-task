/**
 * Copyright (c) 2012, Kinvey, Inc. All rights reserved.
 *
 * This software contains valuable confidential and proprietary information of
 * KINVEY, INC and is subject to applicable licensing agreements.
 * Unauthorized reproduction, transmission or distribution of this file and its
 * contents is a violation of applicable laws.
 *
 * Author: mjsalinger
 */

"use strict";
var redisClient = require('./redis_client')
  , util = require('util')
  , EventEmitter = require('events').EventEmitter
  , utils = require('./utils')
  , os = require('os');


exports.initialize = function(options, callback) {

  var DataStore;

  DataStore = function(options) {
    EventEmitter.call(this);

    if (!options){
      options = {};
    }

    this.redis = redisClient.initialize(options, callback);

    this.limitStore = {
      increment: limitStoreIncrement,
      decrement: limitStoreDecrement,
      cleanup: limitCleanup,
      redis: this.redis
    };

    this.blacklistCounterStore = {
      increment: blacklistCounterIncrement,
      redis: this.redis
    };

    this.blacklistStore = {
      add: blacklistStoreAdd,
      check: blacklistStoreCheck,
      log: blacklistStoreLog,
      redis: this.redis
    };
    this.messageStore = {
      create: messageStoreCreate,
      process: messageStoreProcess,
      redis: this.redis
    };
  };

  util.inherits(DataStore, EventEmitter);

  DataStore.prototype.shutdown = function() {
    var that = this;
    that.redis.shutdown();
  };

  // TODO:  Better abstraction for cleanup

  var limitCleanup = function(prefix, hostname, callback) {
    var that = this
      , currentKey
      , currentValue;

    if (!callback){
      callback = function(){};
    }

    prefix = that.keyPrefix + "*";
    that.redis.getKeys(prefix, function(keys) {
      if (keys instanceof Error) {
        return callback();
      }
      var cleanKeyFunctions = [];
      if (!keys || keys.length == 0) {
        return callback();
      } else {
        for (var i = 0; i < keys.length; i++) {
          cleanKeyFunctions.push(function (finishedKey) {
            currentKey = keys[i];

            that.redis.getRange(currentKey, 0, -1, function (err, values) {
              var removeTaskFunctions = [];
              for (var j = 0; j < values.length; j++) {
                var currentHost;
                try {
                  currentHost = JSON.parse(values[j]).host.toString();
                } catch (err) {
                  currentHost = "";
                }
                if (currentHost === hostname) {

                  currentValue = values[j];
                  removeTaskFunctions.push(function (finishedRemove) {
                    that.redis.removeKeys(currentKey, 0, currentValue, function () {
                      finishedRemove();
                    });
                  });
                }
              }
              async.parallel(removeTaskFunctions, function (err, result) {
                finishedKey();
              });
            });
          });
          async.parallel(cleanKeyFunctions, function (err, result) {
            callback();
          });
        }
        return null;
      }
    });
  };

  var limitStoreIncrement = function(key, message, max, callback) {
    var that = this;

    // TODO Precondition checks

    that.redis.checkListMaxReached(key, max, function(err, result) {
      if (err) {
        callback(err);
      } else {
        if (!result) {
          that.redis.incrementList(key, message, function(err, result) {
            utils.processResult(err, result, callback);
          });
        } else {
          callback(new Error("Too many tasks"));
        }
      }
    });
  };

  var limitStoreDecrement = function(key, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.decrementList(key, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  var blacklistCounterIncrement = function(key, timeout, max, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.getKey(key, function(err, result) {
      if (err) {
        callback(err);
      } else if (result >= 1) {
        that.redis.incrementExpiringKey(key, function(err, result) {
          utils.processResult(err, result, callback);
        });
      } else {
        that.redis.createExpiringKey(key, timeout, "1", function(err, result) {
          utils.processResult(err, result, callback);
        });
      }
    });
  };

  var blacklistStoreAdd = function(key, timeout, value, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.createExpiringKey(key, timeout, value, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  var blacklistStoreCheck = function(key, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.getKey(key, function(err, result) {
      if (err) {
        callback(err);
      }
      if (result) {
        that.redis.getTtlStatus(key, function(err, reply) {
          if (reply) {
            reply.result = result;
          }
          utils.processResult(err, reply, callback);
        });
      } else {
        callback(null, {status: false, timeRemaining: -1, result: ""});
      }
    })
  };

  var blacklistStoreLog = function(key, value, callback) {
    var that = this;

    that.redis.push(key, value, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  var messageStoreCreate = function(hash, id, message, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.setHashKey(hash, id, message, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  var messageStoreProcess = function(hash, id, callback) {
    var that = this;
    // TODO Precondition checks

    that.redis.getAndDeleteHashKey(hash, id, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  return new DataStore(options);

};

