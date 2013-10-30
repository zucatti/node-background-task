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
  , notification = require('./notification_bus')
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

    var that = this;

    this.notificationBus = new notification.initialize(options);
    this.notificationBus.on('error', utils.wrapError(this));
    this.notificationBus.on('notification_received', function(id) {
      that.emit('TASK_AVAILABLE', id.id);
    });

    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  util.inherits(TaskServer, EventEmitter);

  TaskServer.prototype.shutdown = function () {
    var that = this;

    if (!that.isAvailable) {
      return; // Nothing to do
    }

    that.isAvailable = false;
    // Hard end, don't worry about shutting down
    // gracefully here...

    that.notificationBus.shutdown();

    that.removeAllListeners();
  };

  TaskServer.prototype.acceptTask = function (id, callback) {
    var that=this
      , message
      , status;


    status = "NEWTASK";

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

    that.notificationBus.processNotification(id, status, function(err, reply) {
      if (err) {
        if (reply.message === "DB doesn't recognize message" || reply.message.match(/^No message for id/)) {
          callback(new Error('Task not in database, do not accept'));
        }
      }

      that.idToChannelMap[id] = reply._listenChannel;

      if (reply.taskDetails) {
        callback(reply.taskDetails);
      } else {
        callback(reply);
      }
    });
  };

  TaskServer.prototype.reportTask = function (taskId, status, msg) {
    var that = this
      , msgToSend, serializableError, o;

    if (!taskId || !status || !msg){
      throw new Error("Missing msgId, status or msg.");
    }

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
    that.notificationBus.sendNotification(that.idToChannelMap[taskId], taskId, msgToSend, status, function(err, result) {
    });
  };

  TaskServer.prototype.completeTask = function (taskId, status, msg) {
    if (status != "SUCCESS" && status != "ERROR" && status != "FAILED") {
      throw new Error(status + " is not a valid status for completeTask.");
    }
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