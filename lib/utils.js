"use strict";

exports.wrapError = function (emitter) {
  return function (error) {
    emitter.emit('error', error);
  };
};

exports.processResult = function(err, result, callback) {
  if (callback) {
    if (err) {
    } else {
      callback(null, result);
    }
  }
};