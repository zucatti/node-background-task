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
  , message = require('./messaging')
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

    this.timeout = 5000; // 5 second defualt timeout
    if (options && options.timeout) {
      this.timeout = options.timeout;
    }

    // TODO:  fix upon fixing message, but for now...

    this.msgBus = new message.connect(options);
    this.msgBus.on('error', utils.wrapError(this));

    // Simple way to ensure we're not shut down
    this.isAvailable = true;
  };

  util.inherits(TaskClient, EventEmitter);

  TaskClient.prototype.end = function () {
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
    that.msgBus.shutdown();
    that.removeAllListeners();
  };

  TaskClient.prototype.addTask = function (msg, options, completionCallback, progressCallback) {
    var that = this
      , id = message.makeId()
      , cb, timeoutId, timedoutCb, msgToSend, tmpErr, startTheTask, taskTimeout,
      getType = {};
    if (options && getType.toString.call(options) == '[object Function]') {
      progressCallback = completionCallback;
      completionCallback = options;
      options = {};
    }

    if (options && options.taskTimeout && options.taskTimeout > 0) {
      taskTimeout = options.taskTimeout;
    } else {
      taskTimeout = that.timeout;
    }

    if (!that.isAvailable) {
      completionCallback(id, new Error("Attempt to use invalid BackgroundTask"));
      return id;
    }


    startTheTask = function () {
      that.taskLimit.startTask(msg, function (tasks) {
        var err, progressCb, responseCb;
        if (tasks instanceof Error) {
          err = new Error('Too many tasks');
          that.emit('TASK_ERROR', err);
          completionCallback(id, err);
          return;
        }

        callbacks[id] = completionCallback;

        progressCb = function (resp) {
          var rply = extractResponse(resp);

          that.emit('TASK_PROGRESS', rply.id, rply.details);

          if (progressCallback) {
            progressCallback(rply.id, rply.details);
          }
        };

        responseCb = function (resp) {
          var uniqueIndex = id // Make this callback unique
            , rply = extractResponse(resp);

          that.emit('TASK_DONE', rply.id, rply.details);
        };

        cb = function (reply) {
          var origCallback
            , tid = id
            , details = reply
            , rply, fallback = false;

          try {
            rply = extractResponse(reply);
            details = rply.details;
            tid = rply.id;
          } catch (e) {
            // The system had an error
            that.emit('TASK_ERROR', e);
          }
          origCallback = callbacks[tid];

          that.taskLimit.stopTask(msg);
          clearTimeout(timeoutId);
          origCallback(tid, details);
          delete callbacks[tid];
          that.msgBus.removeListener('responseReady:' + id, responseCb);
          that.msgBus.removeListener('progress:' + id, progressCb);
        };

        timedoutCb = function () {
          var origCallback = callbacks[id];
          // replace the "orig" callback with an empty function
          // in case the request still completes in the future and
          // tries to call our callback.
          callbacks[id] = function (reply) {
          };

          // Return an error
          that.taskLimit.stopTask(msg);
          origCallback(id, makeTimeoutError());
          that.msgBus.removeListener('responseReady:' + id, responseCb);
          that.msgBus.removeListener('progress:' + id, progressCb);
        };

        msgToSend = {
          taskId: id,
          taskDetails: msg
        };

        timeoutId = setTimeout(timedoutCb, taskTimeout);
        that.msgBus.sendMessage(id, msgToSend, cb);

        that.msgBus.once('responseReady:' + id, responseCb);
        that.msgBus.on('progress:' + id, progressCb);
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