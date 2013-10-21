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

var uuid = require('node-uuid')
  , async = require('async')
  , util = require('util')
  , utils = require('./utils')
  , redis = require('./redis_client_old')
  , EventEmitter = require('events').EventEmitter
  , connect
  , MessageBus
  , makeId
  , initResponder
  , initCreator
  , wrapError
  , authClient
  , completeTransaction
  , KILOBYTES = 1024
  , MEGABYTES = 1024 * 1024
  , log;


log = function(msg){
    var d = new Date()
      , t = d.toISOString();
    util.debug(t + ": " + msg);
};

wrapError = function(that, evt){
  that.emit('error', evt);
};


exports.makeId = makeId = function(){
    return uuid().replace(/-/g, "");
};

initCreator = function(that){
    that.subClient.subscribe(that.listenChannel);
    that.subClient.on('taskMessage', function(channel, message){
        completeTransaction(that, channel, message);
    });
};


initResponder = function(that){
    that.subClient.subscribe(that.broadcastChannel);
    that.subClient.on('taskMessage', function(channel, message){
        var msgBody, id;
        if (channel !== that.broadcastChannel){
            // This shouldn't happen, it would mean
            // that we've accidentally subscribed to
            // an extra redis channel...
            // If for some crazy reason we get an incorrect
            // channel we should ignore the message
            return;
        }
        id = message;
        that.emit('data_available', id);
    });
};

exports.MessageBus = MessageBus = function(options, callback){    // optional callback for auth test
    var that = this;

    if (!options){
        options = {};
    }

    //EventEmitter.call(this);
    this.idToChannelMap = {};
    this.callbackMap = {};
    this.listenChannel = "msgChannels:" + makeId();
    this.broadcastChannel = (options && options.broadcast) || "msgChannels:broadcast";
    this.dataHash = (options && options.dataHash) || "msgTable:normal";
    this.responseHash = (options && options.outputHash) || "responseTable:normal";
    this.progressHash = (options && options.progressHash) || "progressResponseTable:normal"
    this.shutdownFlag = false;
    this.id = makeId();        // For debugging / logging

    if (options && options.host){ this.redisHost = options.host; }
    if (options && options.port){ this.redisPort = options.port; }
    if (options && options.password){ this.redisPassword = options.password; }


    // optional callback added for auth test
    var functions = [
      function(cb) {that.subClient = redis.createClient(options, cb);},
      function(cb) {that.pubClient = redis.createClient(options, cb);},
      function(cb) {that.dataClient = redis.createClient(options, cb);}
    ];

    var finalCallback = function() {
      if (callback) {
        callback();
      }
    };

    async.parallel(functions, finalCallback);

    if (options && options.isResponder){
        initResponder(this);
    } else {
        initCreator(this);
    }

    this.dataClient.on('taskError', function(evt){wrapError(that, evt);});
    this.pubClient.on('taskError', function(evt){wrapError(that, evt);});
    this.subClient.on('taskError', function(evt){wrapError(that, evt);});
};

util.inherits(MessageBus, EventEmitter);

completeTransaction = function(that, ch, msg){
    var id, status, parsedMsg, callback, hash;

    if (ch !== that.listenChannel){
        // This can NEVER happen (except for if there's an error)
        // in our code.  So the check is left in, but
        // should never be hit.
        that.emit('error', new Error("Got message for some other channel (Expected: " +
                                     that.listenChannel + ", Actual: " + ch + ")"));
        // Bail out!
        return;
    }

    parsedMsg = msg.split(' ');
    if (parsedMsg.length < 2){
        that.emit('error',  new Error("Invalid message received!"));
        // Bail out
        return;
    }

    id = parsedMsg[0];
    status = parsedMsg[1];

    hash = status === 'PROGRESS' ? that.progressHash : that.responseHash;

    callback = that.callbackMap[id];

    that.dataClient.getAndDelete(hash, id, function(err, result) {
      if (err) {
        that.emit('error', err);
      } else {
        if (status === 'PROGRESS') {
          that.emit('progress:' + id, result);
          return;
        }
        if (status !== 'SUCCESS') {
          // build the error
          that.emit('error', result);
        }

        // If we're hard failed we stop working here.
        if (status !== "FAILED") {
          that.emit('responseReady:' + id, result);
        }
        if (callback) {
          callback(result);
          delete that.callbackMap[id];
        }
      }
    });
};

MessageBus.prototype.sendMessage = function(id, msg, callback){
    var that = this
      , msgString;

    if (this.shutdownFlag){
        callback(new Error("Attempt to use shutdown MessageBus."));
        return;
    }

    msg._listenChannel = that.listenChannel;
    msg._messageId = id; // Store the message id in-band

    msgString = JSON.stringify(msg);

    if (!msgString){
        callback(new Error("Error converting message to JSON."));
        return;
    }

    // TODO: This needs to be an option for the class
    if (msgString.length > MEGABYTES){
        callback(new Error("Payload too large!"));
        return;
    }


    this.dataClient.setMessage(this.dataHash, id, msgString, function(err, reply){
        if (err){
          callback(new Error("Error sending message: " + err));
        } else {
          that.pubClient.publishKey(that.broadcastChannel, id, function() {
            that.callbackMap[id] = callback;
          });
        }
    });
};

MessageBus.prototype.acceptMessage = function(mid, callback){
    var that = this
      , listenChannel
      , id
      , derivedId;

    if (!mid){
        throw new Error('Missing Message ID.');
    }

    if (!callback){
        throw new Error('Invalid callback.');
    }

    if (callback.length < 1){
        throw new Error('Missing parameters in callback.');
    }

    that.dataClient.getAndDelete(that.dataHash, mid, function(err, reply){
        if (err) {
          callback(err);
        } else {
          derivedId = id = reply._messageId;
          listenChannel = reply._listenChannel;

          if (id !== mid){
              console.log("ERROR: Mis-match on ids! (" + id + " does not equal " + mid + ")");
          }

          that.idToChannelMap[id] = listenChannel;


          delete reply._listenChannel;
          delete reply._messageId;

          callback(reply);
        }

    });
};

MessageBus.prototype.sendResponse = function(msgId, status, msg){
    if (!msgId || !status || !msg){
        throw new Error("Missing msgId, status or msg.");
    }

    if (status !== 'SUCCESS' &&
        status !== 'ERROR'   &&
        status !== 'FAILED'  &&
        status !== 'PROGRESS')
    {
        throw new Error(status + ' is not a valid status.');
    }

    var listenChannel = this.idToChannelMap[msgId]
      , that = this, serializableError, o, tmpErr, testMode, hash;

    if (arguments.length === 4){
        testMode = arguments[3];
    }

    if (!listenChannel && !testMode){
        // We have no record of this request
        // probably a callback being called twice, but
        // still need to throw
        tmpErr = new Error('Attempt to respond to message that was never accepted');
        tmpErr.debugMessage = msgId + " is not registered as having been accepted";
        throw tmpErr;
    }

    // Concurrency control: when sending n responses concurrently, `hset` is
    // invoked n times before being published n times. However, since `hset`
    // modifies a (one) key, it doesn’t make sense to publish n times. So,
    // only publish when the last `hset` of n completes.
    that.pending        = that.pending || { };
    that.pending[msgId] = (that.pending[msgId] || 0) + 1;

    hash = status === 'PROGRESS' ? that.progressHash : that.responseHash;

    that.dataClient.setMessage(hash, msgId, JSON.stringify(msg), function(err, reply) {
      if (err) {
        that.pending[msgId] -= 1;
        throw new Error(err);
      }
      if(1 === that.pending[msgId]) {
        that.pubClient.publishKey(listenChannel, msgId + " " + status, function() {
          that.pending[msgId] -= 1;
        });
      } else {// There are concurrent operations, do not publish.
        that.pending[msgId] -= 1;
      }
      if(status !== 'PROGRESS') {
        delete that.idToChannelMap[msgId];
      }

    });
};


MessageBus.prototype.shutdown = function(){
    this.subClient.removeAllListeners();
    this.dataClient.removeAllListeners();
    this.pubClient.removeAllListeners();
    this.subClient.removeAllListeners();
    this.dataClient.end();
    this.pubClient.end();
    this.subClient.end();
    this.removeAllListeners();
    this.shutdownFlag = true;
};

exports.connect = connect = function(options, callback){
    return new MessageBus(options, callback);
};
