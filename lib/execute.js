/*
 *  execute.js
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
 *  Requires: self.postgres, self.statement
 *  Produces: self.postgres_result, self.jsons, self.json, self.count
 *
 *  Execute a Postgres statement. The rows coming back are
 *  not pure Dictionaries so we do a little munging.
 */
const execute = _.promise.make((self, done) => {
    const method = "execute";

    assert.ok(self.postgres, `${method}: expected self.postgres`)
    assert.ok(self.statement, `${method}: expected self.statement`)

    if (self.verbose) {
        console.log("-", "postgres.execute", self.statement)
    }

    self.postgres.query(self.statement, self.params)
        .then(result => {
            self.postgres_result = result;

            let last_result = result;
            if (_.is.Array(result)) {
                last_result = result[result.length - 1];

            }
            self.jsons = last_result.rows.map(d => Object.assign({}, d));
            self.count = last_result.rowCount;
            self.json = self.jsons.length ? self.jsons[0] : null;

            done(null, self)
        })
        .catch(done)
})

/**
 *  Parameterized
 */
const execute_p = (statement, params) => _.promise.make((self, done) => {
    _.promise.make(self)
        .then(_.promise.add({
            statement: statement,
            params: params || [],
        }))
        .then(execute)
        .then(_.promise.done(done, self, "postgres_result,jsons,json,count"))
        .catch(done)
})

/**
 *  API
 */
exports.execute = execute;
exports.execute.p = execute_p;
