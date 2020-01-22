/**
 *  test/execute.js
 *
 *  David Janes
 *  IOTDB
 *  2018-01-25
 *
 *  Copyright (2013-2020) David P. Janes
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

"use strict";

const _ = require("iotdb-helpers");

const assert = require("assert");

const postgres = require("..")
const _util = require("./_util")

describe("execute", function() {
    let self = {}

    before(function(done) {
        _.promise.make(self)
            .then(_util.initialize)
            // .then(_util.load)
            .then(_.promise.make(sd => {
                self = sd;
            }))
            .then(_.promise.done(done))
            .catch(done)
    })

    describe("good", function() {
        it("works", function(done) {
            _.promise.make(self)
                .then(_.promise.optional(postgres.execute.p("DROP TABLE items")))
                .then(postgres.execute.p("CREATE TABLE items(id SERIAL PRIMARY KEY, text VARCHAR(40) not null, complete BOOLEAN)"))
                .then(postgres.execute.p("INSERT INTO items(text, complete) values($1, $2)", [ "hello", true ]))
                .then(postgres.execute.p("SELECT * FROM items"))
                .then(_.promise.make(sd => {
                    assert.ok(sd.jsons)
                    assert.ok(sd.json)
                    assert.deepEqual(sd.jsons.length, 1)
                    assert.deepEqual(sd.count, 1)
                }))
                .then(_.promise.done(done))
                .catch(done)
        })
    })
})
