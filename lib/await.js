/*
 *  await.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-01-24
 *
 *  Copyright [2013-2018] [David P. Janes]
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

const postgres = require("pg");

const assert = require("assert");

/**
 *  Requires: self.postgresd
 *  Produces: self.postgres
 */
const await = _.promise.make((self, done) => {
    const method = "await";

    assert.ok(self.postgres_query, `${method}: expected self.postgres_query`)

    self.postgres_query.on("end", () => {
        done(null, self)
        done = _.noop;
    })
    self.postgres_query.on("error", error => {
        done(error, self)
        done = _.noop;
    })
})

/**
 *  API
 */
exports.await = await;
