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

var RedisClient
  , redis = require('redis')
  , util = require('util')
  , utils = require('./utils')
  , EventEmitter = require('events').EventEmitter;

exports.initialize = function(options, callback) {
  RedisClient = function(options, callback){    // optional callback for auth test
    EventEmitter.call(this);

    if (options.host){ this.redisHost = options.host; }
    if (options.port){ this.redisPort = options.port; }
    if (options.password){ this.redisPassword = options.password; }

    this.client = redis.createClient(this.redisPort, this.redisHost);

    this.client.on('ready', function() {
      this.emit('clientReady');
    });

    this.client.on('error', function(evt) {
      console.log("got error!");
      console.log(evt);
      this.emit('taskError', evt);
    })


    if (options.password){
      this.client.auth(options.password, function() {
        // this callback is used primarily for testing redis auth
        if (callback) {
          callback();
        }
      });
    }
  };

  util.inherits(RedisClient,EventEmitter);

  RedisClient.prototype.checkListMaxReached = function(key, max, callback) {
    var that = this;

    // TODO:  Add preconditions

    that.client.llen(key, function(err, len) {
      if (err) {
        callback(err);
      } else {
        if (len >= max) {
          callback(null, true);
        } else {
          callback(null, false);
        }
      }
    });
  };

  RedisClient.prototype.getKeys = function(prefix, callback) {
    var that = this;
    that.client.keys(prefix, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.getRange = function(key, start, stop, callback) {
    var that = this;
    that.client.lrange(key, 0, -1, function (err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, result);
      }
    });
  };

  RedisClient.prototype.removeKeys = function(key, count, value, callback) {
    var that = this;
    that.client.lrem(key, count, value, function(err, result) {
      if (err) {
        callback(err);
      } else {
        callback(result);
      }
    });
  };

  RedisClient.prototype.incrementList = function (key, message, callback) {
    var that = this;

    // TODO:  Add preconditions
    that.client.lpush(key, message, function(err, result){
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.decrementList = function(key, message, callback) {
    var that = this;

    // TODO:  Add preconditions
    that.client.lpop(key, message, function(err, result){
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.getKey = function(key, callback) {
    var that = this;

    that.client.get(key, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.createExpiringKey = function(key, timeout, value, callback) {
    var that = this;

    // TODO:  Add preconditions
    that.getKey(key, function(err, result) {
      if (err) {
        callback(err);
      } else {
        if (result) {
          callback(null, result);
        } else {
          that.client.setex(key, timeout, value, function(err, reply) {
            // Passing value back as result because reply is "OK, which is meaningless, and value can be used to
            // increment if an integer.
            utils.processResult(err, value, callback);
          });
        }
      }
    });
  };

  RedisClient.prototype.incrementExpiringKey = function(key, callback) {
    var that = this;

    // TODO:  Add preconditions
    that.client.incr(key, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.getTtlStatus = function(key, callback) {
    var that = this;

    // TODO:  Add preconditions

    that.client.ttl(key, function(error, timeRemaining){
      if (error) {
        callback(error)
      }
      if (timeRemaining){
        callback(null, {status: true, timeRemaining: timeRemaining});
      } else {
        callback(null, {status: true, timeRemaining: -1});
      }
    });
  };

  RedisClient.prototype.push = function(key, value, callback) {
    var that = this;

    that.client.rpush(key, value, function(err, result) {
      if (err) {
        callback(err);
      } else {
        callback(null, result);
      }
    });
  };

  RedisClient.prototype.subscribe = function(channel, callback) {
    var that = this;

    // TODO:  Add preconditions

    that.client.subscribe(channel, function(err) {
      if (err) {
        if(callback) {
          callback(err);
        }
      } else {
        that.client.on('message', function(channel, message) {
          that.emit('taskMessage', channel, message)
        });
        if (callback) {
          callback();
        }
      }
    });
  };

  RedisClient.prototype.getAndDeleteHashKey = function(hash, key, callback) {
    var that = this;
    var multi = that.client.multi();

    multi.hget(hash, key)
      .hdel(hash, key)
      .exec(function(err, result) {
        var myErr = null, msgBody;

        if (err){
          myErr = new Error("REDIS Error: " + err);
        }

        if (!util.isArray(result) && !myErr){
          myErr = new Error("Internal REDIS error (" + err + ", " + result + ")");
        } else if (util.isArray(result)){
          // Reply[0] => hget, Reply[1] => hdel
          result = result[0];
        }

        if (result === null && !myErr){
          myErr = new Error("No message for key " + key);
        }

        if (!myErr){
          try {
            msgBody = JSON.parse(result);
          } catch(e){
            myErr = new Error("Bad data in sent message, " + e);
          }
        }

        if (!myErr && !msgBody) {
          myErr = new Error("DB doesn't recognize message");
        }

        utils.processResult(myErr, msgBody, callback);
      });
  };

  RedisClient.prototype.setHashKey = function (hash, key, value, callback) {
    var that = this;
    that.client.hset(hash, key, value, function(err, result) {
      if (err){
        err = new Error("Error sending message: " + err);
      }
      utils.processResult(err, result, callback);
    });
  };

  RedisClient.prototype.publishKey = function(channel, key, callback) {
    var that = this;

    that.client.publish(channel, key, function() {
      callback();
    });
  };

  RedisClient.prototype.shutdown = function() {
    this.client.removeAllListeners();
    this.client.end();
  }

  return new RedisClient(options, callback);

};