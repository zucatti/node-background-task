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

var dataStore = require('./data_store')
  , utils = require('./utils')
  , util = require('util')
  , EventEmitter = require('events').EventEmitter;

exports.initialize = function(options, callback) {

  var NotificationBus;

  NotificationBus = function(options, callback){
    EventEmitter.call(this);
  };

  util.inherits(NotificationBus, EventEmitter);

  NotificationBus.prototype.sendNotification = function() {

  };

  NotificationBus.prototype.receiveNotification = function() {

  };

  NotificationBus.prototype.shutdown = function() {

  };

  return new NotificationBus(options, callback);
};

