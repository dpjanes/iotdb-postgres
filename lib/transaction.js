/*
 *  transaction.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-08-20
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
 *  Requires: self.postgres
 */
const transaction = paramd => _.promise.make((self, done) => {
    const method = "transaction";
    const postgres = require("..");

    if (_.is.Function(paramd)) {
        paramd = {
            method: paramd,
        }
    }

    assert.ok(self.postgres, `${method}: expected self.postgres`)
    assert.ok(paramd.method, `${method}: expected paramd.method`)

    _.promise.make(self)
        .then(_.promise.add("rollback", false))
        .then(postgres.execute.p("BEGIN"))

        .then(paramd.method)

        .catch(error => {
            if (!error.self) {
                throw error
            }

            error.self.rollback = true
            error.self.rollback_error = error

            return error.self
        })
        .then(_.promise.conditional(
            sd => sd.rollback,
            postgres.execute.p("ROLLBACK"),
            postgres.execute.p("COMMIT")
        ))
        .then(_.promise.conditional(
            sd => sd.rollback_error,
            _.promise.make(sd => {
                throw sd.rollback_error
            })
        ))
        .then(_.promise.done(done))
        .catch(done)


})

/**
 *  API
 */
exports.transaction = transaction;