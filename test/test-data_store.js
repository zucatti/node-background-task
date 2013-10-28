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

var sinon = require('sinon')
  , dataStore = require('../lib/data_store')
  , redis = require('redis')
  , should = require('should')
  , task = {a: "kid1234", msg: "Hi Mom!"}
  , async = require('async');

describe('Test Data Store', function() {
  var rc;

  before(function(done) {
    dataStore = new dataStore.initialize({taskKey: "a",  maxTasksPerKey: 5});
    rc = redis.createClient();
    rc.flushall();
    task = {a: "kid1234", msg: "Hi Mom!"};
    done();
  });

  beforeEach(function(done) {
    rc.flushall();
    done();
  });

  describe('Error Handling', function(){
    it.only('should not create an object without a task key', function(){
      (function(){
        var hi = dataStore.initialize();
      }).should.throw('I need a task key!');
    });

    it('should not start invalid tasks', function(){
      var tl = new limits.TaskLimit({taskKey: 'a'})
        , cb = function(res){
          res.should.be.an.instanceOf(Error);
          res.message.should.equal('Invalid task, not running.');
        };

      tl.startTask(null, cb);
      tl.startTask(undefined, cb);
      tl.startTask({}, cb);
      tl.startTask({b: "x"}, cb);

    });

    it('should not stop invalid tasks', function(){
      var tl = new limits.TaskLimit({taskKey: 'a'});
      (function(){tl.stopTask(null); }).should.throw("Invalid task, can't stop.");
      (function(){tl.stopTask(undefined); }).should.throw("Invalid task, can't stop.");
      (function(){tl.stopTask({}); }).should.throw("Invalid task, can't stop.");
      (function(){tl.stopTask({b: "x"}); }).should.throw("Invalid task, can't stop.");

    });

  });



});