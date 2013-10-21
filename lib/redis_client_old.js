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

exports.createClient = (function() {

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

  RedisClient.prototype.incrementWithMax = function(key, message, max, callback) {
    var that = this;

    // Add test for values

    that.client.llen(key, function(err, len){
      if (len >= max){
        callback(new Error("Too many tasks"));
      } else {
        that.client.lpush(key, message, function(x){
          callback(null, len+1);
        });
      }
    });
  };

  RedisClient.prototype.decrement = function(key, callback) {
    var that = this;
    that.client.lpop(key, function(err, result) {
      if (err) {
        callback(err);
      } else {
        callback(result);
      }
    });
  };

  RedisClient.prototype.getKeys = function(prefix, callback) {
    var that = this;
    that.client.keys(prefix, function(err, result) {
      if (err) {
        callback (err);
      } else {
        callback(result);
      }
    });
  };

  RedisClient.prototype.ttlKeyStatus = function(key, callback) {
    var that = this;
    that.client.get(key, function(err, result) {
      if (err) {
        callback(err);
      }
      if (result){
        // We're blacklisted
        that.client.ttl(key, function(error, timeRemaining){
          if (error) {
            callback(error)
          }
          if (timeRemaining){
            callback(null, {status: true, timeRemaining: timeRemaining, result: result});
          } else {
            callback(null, {status: true, timeRemaining: -1, result: result});
          }
        });
      } else {
        callback(null, {status: false, timeRemaining: -1, result: ""});
      }
    });
  };

  RedisClient.prototype.incrementExpiringKey = function(key, interval, value, callback) {
    var that = this;

    that.client.get(key, function(err, result) {
      if (err) {
        callback(err);
      } else {
        if (!result) {
          // Key not in redis, so we will create
          that.client.setex(key, interval, value, function(err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
        } else {
          that.client.incr(key, function(err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
        }
      }
    });
  };

  RedisClient.prototype.setExpiringKey = function(key, interval, value, callback) {
    var that = this;
    that.client.get(key, function(err, result) {
      if (err) {
        callback(err);
      } else {
        if (result) {
          callback(null, result);
        } else {
          that.client.setex(key, interval, value, function(err, reply) {
            if (err) {
              callback(err);
            } else {
              callback(null, reply);
            }
          });
        }
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

  RedisClient.prototype.subscribe = function(channel, callback) {
    var that = this;
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

  RedisClient.prototype.getAndDelete = function(channel, id, callback) {
    var that = this;
    var multi = that.client.multi();

    multi.hget(channel, id)
      .hdel(channel, id)
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
          myErr = new Error("No message for id " + id);
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

        if (myErr){
          callback(myErr);
        } else {
          callback(null, msgBody);
        }

      });
  };

  RedisClient.prototype.setMessage = function (channel, key, message, callback) {
    var that = this;
    that.client.hset(channel, key, message, function(err, result) {
      if (err){
        callback(new Error("Error sending message: " + err));
        return;
      } else {
        callback();
      }
    });

  };

  RedisClient.prototype.publishKey = function(channel, key, callback) {
    var that = this;

    that.client.publish(channel, key, function() {
      callback();
    });
  };

  RedisClient.prototype.end = function() {
    this.client.removeAllListeners();
    this.client.end();
  }

  return function(options, callback) {
    return new RedisClient(options, callback);
  };

})();


