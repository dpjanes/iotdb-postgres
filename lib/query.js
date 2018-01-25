/*
 *  query.js
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

/*
 *  Requires: self.postgres, self.query
 */
const query = _.promise.make((self, done) => {
    const method = "query";

    assert.ok(self.postgres, `${method}: expected self.postgres`)
    assert.ok(self.query, `${method}: expected self.query`)

    self.postgres.query(self.query, self.params)
        .then(result => {
            self.postgres_result = result;
            self.rows = result.rows;
            self.count = result.rowCount;

            done(null, self)
        })
        .catch(done)
})

/**
 */
const query_p = (query_, params) => _.promise.make((self, done) => {
    _.promise.make(self)
        .then(_.promise.add({
            query: query_,
            params: params || [],
        }))
        .then(query)
        .then(_.promise.done(done, self, "postgres_result,rows,count"))
        .catch(done)
})

/**
 *  API
 */
exports.query = query;
exports.query.p = query_p;
