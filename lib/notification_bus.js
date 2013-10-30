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

var uuid = require('node-uuid')
  , dataStore = require('./data_store')
  , redis = require('./redis_client')
  , utils = require('./utils')
  , util = require('util')
  , EventEmitter = require('events').EventEmitter
  , initCreator
  , initResponder
  , MEGABYTES = 1024 * 1024
  , async = require('async')
  , makeId;

exports.makeId = makeId = function(){
  return uuid().replace(/-/g, "");
};

initCreator = function(that){
  that.subClient.subscribe(that.listenChannel);
  that.subClient.on('taskMessage', function(channel, message){
    var msgObj;

    msgObj = JSON.parse(message);

    that.emit('notification_received', msgObj);
  });
};


initResponder = function(that){
  that.subClient.subscribe(that.broadcastChannel);
  that.subClient.on('taskMessage', function(channel, message){
    var msgObj;
    if (channel !== that.broadcastChannel){
      // This shouldn't happen, it would mean
      // that we've accidentally subscribed to
      // an extra redis channel...
      // If for some crazy reason we get an incorrect
      // channel we should ignore the message
      return;
    }
    msgObj = JSON.parse(message);
    that.emit('notification_received', msgObj);
  });
};


exports.initialize = function(options, callback) {

  var NotificationBus;

  NotificationBus = function(options, callback){
    EventEmitter.call(this);

    var that = this;

    if (!options){
      options = {};
    }

    //EventEmitter.call(this);

    this.listenChannel = "msgChannels:" + makeId();
    this.baseHash = (options && options.baseHash) || ":normal";
    this.broadcastChannel = (options && options.broadcast) || "msgChannels:broadcast";
    /*this.dataHash = (options && options.dataHash) || "msgTable:normal";
    this.responseHash = (options && options.outputHash) || "responseTable:normal";
    this.progressHash = (options && options.progressHash) || "progressResponseTable:normal"*/
    this.shutdownFlag = false;
    this.id = makeId();        // For debugging / logging

    // optional callback added for auth test
    var functions = [
      function(cb) {that.subClient = redis.initialize(options, cb);},
      function(cb) {that.pubClient = redis.initialize(options, cb);},
      function(cb) {that.dataClient = dataStore.initialize(options, cb);}
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

    //this.dataClient.on('taskError', function(evt){wrapError(that, evt);});
    //this.pubClient.on('taskError', function(evt){wrapError(that, evt);});
    //this.subClient.on('taskError', function(evt){wrapError(that, evt);});
  };

  util.inherits(NotificationBus, EventEmitter);

  NotificationBus.prototype.sendNotification = function(channel, id, message, status, callback) {
    var that = this
      , msgString
      , pubMessage
      , hash
      , getType = {};

    if (this.shutdownFlag){
      callback(new Error("Attempt to use shutdown Notification Bus."));
      return;
    }

    if (status && getType.toString.call(status) == '[object Function]') {
      callback = status;
      status = "message";
    }

    message._listenChannel = that.listenChannel;
    message._messageId = id; // Store the message id in-band
    message._status = status;

    msgString = JSON.stringify(message);

    if (!msgString){
      callback(new Error("Error converting message to JSON."));
      return;
    }

    // TODO: This needs to be an option for the class
    if (msgString.length > MEGABYTES){
      callback(new Error("Payload too large!"));
      return;
    }

    pubMessage = {};
    pubMessage.id = id;
    pubMessage.status = status;

    hash = status.toLowerCase() + this.baseHash;

    this.dataClient.messageStore.create(hash, id, msgString, function(err, reply){
      if (err){
        callback(new Error("Error sending message: " + err));
      } else {
        that.pubClient.publish(channel, JSON.stringify(pubMessage), function() {
          callback(err, that.listenChannel);
        });
      }
    });
  };

  NotificationBus.prototype.processNotification = function(id, status, callback) {

    var that=this
      , hash;

    hash = status.toLowerCase() + that.baseHash;

    that.dataClient.messageStore.process(hash, id, function(err, result) {
      utils.processResult(err, result, callback);
    });
  };

  NotificationBus.prototype.shutdown = function() {
    this.subClient.removeAllListeners();
    this.pubClient.removeAllListeners();
    this.dataClient.removeAllListeners();
    this.dataClient.shutdown();
    this.pubClient.shutdown();
    this.subClient.shutdown();
    this.removeAllListeners();
    this.shutdownFlag = true;
  };

  return new NotificationBus(options, callback);
};

