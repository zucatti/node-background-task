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

var utils = require('./utils')
  , EventEmitter = require('events').EventEmitter
  , message = require('./messaging')
  , util = require('util')
  , async = require('async');

exports.initialize = (function () {

  var TaskServer;

  TaskServer = function(options) {
    EventEmitter.call(this);

    if (!options) {
      options = {};
    }

    this.idToChannelMap = {};

    options.isResponder = true;
    this.isWorker = true;

    if (options.task) {
      options.broadcast = options.task + "Broadcast";
      options.dataHash = options.task + "Table";
      options.outputHash = options.task + "Hash";
    }

    if (options.taskKey) {
      this.taskKey = options.taskKey;
      if (!options.maxTasksPerKey) {
        this.maxTasksPerKey = options.maxTasksPerKey = 5;
      }

    }

    this.msgBus = new message.connect(options);
    this.msgBus.on('error', utils.wrapError(this));

    var that = this;
    this.msgBus.on('data_available', function (id) {
      that.emit('TASK_AVAILABLE', id);
    });


    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  util.inherits(TaskServer, EventEmitter);

  TaskServer.prototype.end = function () {
    var that = this;

    if (!that.isAvailable) {
      return; // Nothing to do
    }

    that.isAvailable = false;
    // Hard end, don't worry about shutting down
    // gracefully here...

    that.msgBus.shutdown();

    that.removeAllListeners();
  };

  TaskServer.prototype.acceptTask = function (id, callback) {
    var that=this
      , newCallback;

    if (!that.isAvailable) {
      callback(new Error("Attempt to use invalid BackgroundTask"));
      return;
    }

    if (!id || id.length === 0) {
      throw new Error('Missing Task ID.');
    }

    if (!callback || callback.length < 1) {
      throw new Error('Invalid callback specified');
    }

    newCallback = function (reply) {
      if (reply instanceof Error) {
        if (reply.message === "DB doesn't recognize message" || reply.message.match(/^No message for id/)) {
          reply = new Error('Task not in database, do not accept');
        }
      }

      if (reply.taskDetails) {
        callback(reply.taskDetails);
      } else {
        callback(reply);
      }
    };

    that.msgBus.acceptMessage(id, function(reply) {
      newCallback(reply);
    });
  };

  TaskServer.prototype.reportTask = function (taskId, status, msg) {
    var that = this
      , msgToSend, serializableError, o;


    if (!that.isAvailable) {
      throw new Error("Attempt to use invalid BackgroundTask");
    }

    // We can't send Error's via JSON...
    if (msg instanceof Error) {
      serializableError = {
        isError: true,
        message: msg.message
      };

      for (o in msg) {
        if (msg.hasOwnProperty(o)) {
          serializableError[o] = msg[o];
        }
      }

      msg = serializableError;
    }


    msgToSend = {
      taskId: taskId,
      taskDetails: msg
    };

    if (!msg) {
      throw new Error('Missing msgId, status or msg.');
    }

    that.msgBus.sendResponse(taskId, status, msgToSend);
  };

  TaskServer.prototype.completeTask = function (taskId, status, msg) {
    this.reportTask(taskId, status, msg);
  };

  TaskServer.prototype.progressTask = function (taskId, msg) {
    this.reportTask(taskId, 'PROGRESS', msg);
  };

  TaskServer.prototype.reportBadTask = function (taskKey, reason, callback) {
    var reasonToSend;

    if (!this.isAvailable) {
      callback("ERR", "Attempt to use invalid BackgroundTask");
      return;
    }


    if (reason instanceof Error) {
      reasonToSend = reason.message;
    }

    this.blacklist.addFailure(taskKey, reason, callback);

  };

  return function (options) {
    return new TaskServer(options);
  };

})();