/**
 * Copyright (c) 2012, Kinvey, Inc. All rights reserved.
 *
 * This software contains valuable confidential and proprietary information of
 * KINVEY, INC and is subject to applicable licensing agreements.
 * Unauthorized reproduction, transmission or distribution of this file and its
 * contents is a violation of applicable laws.
 * Author: mjsalinger
 */

"use strict";

var utils = require('./utils')
  , util = require('util')
  , EventEmitter = require('events').EventEmitter
  , notification = require('./notification_bus')
  , task_limit = require('./task_limit')
  , blacklist = require('./blacklist');

exports.initialize = (function () {
  var TaskClient
    , callbacks = []
    , extractResponse
    , makeTimeoutError;

  TaskClient = function(options) {
    EventEmitter.call(this);

    if (!options) {
      options = {};
    }

    if (options.task) {
      options.broadcast = options.task + "Broadcast";
      options.dataHash = options.task + "Table";
      options.outputHash = options.task + "Hash";
    }

    this.completionCallbackMap = {};
    this.progressCallbackMap = {};
    this.taskTimeouts = {};
    this.msgMap = {};

    if (options.taskKey) {
      this.taskKey = options.taskKey;
      if (!options.maxTasksPerKey) {
        this.maxTasksPerKey = options.maxTasksPerKey = 5;
      }

      this.taskLimit = new task_limit.TaskLimit(options);
      this.taskLimit.on('error', utils.wrapError(this));

      this.taskLimit.cleanupTasks();

      this.blacklist = new blacklist.Blacklist(options);
      this.blacklist.on('error', utils.wrapError(this));

    }

    this.broadcastChannel = (options && options.broadcast) || "msgChannels:broadcast";
    this.dataHash = (options && options.dataHash) || "msgTable:normal";
    this.responseHash = (options && options.outputHash) || "responseTable:normal";
    this.progressHash = (options && options.progressHash) || "progressResponseTable:normal"

    this.timeout = 5000; // 5 second defualt timeout
    if (options && options.timeout) {
      this.timeout = options.timeout;
    }

    // TODO:  fix upon fixing message, but for now...

    var that = this;

    this.notificationBus = new notification.initialize(options);
    this.notificationBus.on('error', utils.wrapError(this));
    this.notificationBus.on('notification_received', function(task) {
      receiveTransaction(that, task);
    });

    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  util.inherits(TaskClient, EventEmitter);

  var receiveTransaction = function(that, task) {
    var taskCallback
      , status
      , id;

    if (!task.id ) {
      that.emit('error',"Malformed task: " + task);
      delete that.completionCallbackMap[task.id];
      delete that.progressCallbackMap[task.id];
      return;
    }

    if (!task.status) {
      taskCallback = that.completionCallbackMap[task.id];
      if (taskCallback) {
        taskCallback(task.id, new Error("Missing Status for message"));
        delete that.completionCallbackMap[task.id];
        delete that.progressCallbackMap[task.id];
      }
      return;
    }
    status = task.status;
    id = task.id;

    taskCallback = status === "PROGRESS" ? that.progressCallbackMap[id] : that.completionCallbackMap[id];

    that.notificationBus.processNotification(id, status, function(err, result) {
       if (err) {
         taskCallback(id, err);
         return;
       }

      var reply = extractResponse(result);
      if (status === "ERROR" || status === "SUCCESS" || status === "FAILED") {

        that.taskLimit.stopTask(that.msgMap[id]);
        clearTimeout(that.taskTimeouts[id]);
        delete that.msgMap[id];
        delete that.taskTimeouts[id];
        delete that.completionCallbackMap[id];
        delete that.progressCallbackMap[id];
      }

      if (status === "SUCCESS") {
        that.emit('TASK_DONE', reply.id, reply.details);
      }

      if (status === "PROGRESS") {
        that.emit('TASK_PROGRESS', reply.id, reply.details);
      }

      if (taskCallback) {
        taskCallback(reply.id, reply.details);
      }
    });
  };

  TaskClient.prototype.shutdown = function () {
    var that = this;

    if (!that.isAvailable) {
      return; // Nothing to do
    }

    that.isAvailable = false;
    // Hard end, don't worry about shutting down
    // gracefully here...
    if (that.blacklist) {
      that.blacklist.shutdown();
    }
    if (that.taskLimit) {
      that.taskLimit.shutdown();
    }
    that.notificationBus.shutdown();
    that.removeAllListeners();
  };

  TaskClient.prototype.addTask = function (msg, options, completionCallback, progressCallback) {
    var that = this
      , id = notification.makeId()
      , timedoutCb
      , msgToSend
      , tmpErr
      , startTheTask
      , taskTimeout
      , getType = {};

    if (options && getType.toString.call(options) == '[object Function]') {
      progressCallback = completionCallback;
      completionCallback = options;
      options = {};
    }

    if (!progressCallback) {
      progressCallback = function() {};
    }

    taskTimeout = (options && options.taskTimeout && options.taskTimeout > 0) ? options.taskTimeout : that.timeout;

    if (!that.isAvailable) {
      completionCallback(id, new Error("Attempt to use invalid BackgroundTask"));
      return id;
    }

    startTheTask = function() {
      that.taskLimit.startTask(msg, function(tasks) {

        var err;
        if (tasks instanceof Error) {
          err = new Error("Too many tasks");
          that.emit('TASK_ERROR', err);
          completionCallback(id, err);
          return;
        }

        that.completionCallbackMap[id] = completionCallback;
        that.progressCallbackMap[id] = progressCallback;
        that.msgMap[id] = msg;

        timedoutCb = function () {
          var origCallback = that.completionCallbackMap[id];
          // replace the "orig" callback with an empty function
          // in case the request still completes in the future and
          // tries to call our callback.
          callbacks[id] = function (reply) {};

          // Return an error
          that.taskLimit.stopTask(msg);
          origCallback(id, makeTimeoutError());
        };

        msgToSend = {
          taskId: id,
          taskDetails: msg
        };

        that.taskTimeouts[id] = setTimeout(timedoutCb, taskTimeout);

        that.notificationBus.sendNotification(that.broadcastChannel, id, msgToSend, "NEWTASK", function() {

        });
      });
    };

    if (that.taskKey && msg[that.taskKey]) {
      that.blacklist.blacklistStatus(msg, function (isBlacklisted, timeLeft, reason) {
        var tmpErr;
        if (isBlacklisted) {
          tmpErr = new Error('Blacklisted');
          tmpErr.debugMessage = "Blocked, reason: " + reason + ", remaining time: " + timeLeft;
          that.emit('TASK_ERROR', tmpErr);
          completionCallback(id, tmpErr);
        } else {
          startTheTask();
        }
      });
    } else {
      tmpErr = new Error('Missing taskKey');
      that.emit('TASK_ERROR', tmpErr);
      completionCallback(id, tmpErr);
    }
    return id;

  };

  extractResponse = function (r) {
    var response, id, respondedErr, o;

    if (!r.taskId && !r.taskDetails) {
      throw new Error("Incomplete task response.");
    }

    id = r.taskId;
    response = r.taskDetails;

    if (response.isError) {
      respondedErr = new Error(response.message);
      for (o in response) {
        if (response.hasOwnProperty(o) &&
          o !== 'isError' &&
          o !== 'message') {
          respondedErr[o] = response[o];
        }
      }
      response = respondedErr;
    }

    return {id: id, details: response};
  };

  makeTimeoutError = function () {
    return new Error('Task timed out');
  };


  return function (options) {
    return new TaskClient(options);
  };

})();