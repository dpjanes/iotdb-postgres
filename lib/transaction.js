/*
 *  transaction.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-08-20
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

"use strict"

const _ = require("iotdb-helpers")

const postgres = require("pg")

const _begin = _.promise((self, done) => {
    self.postgres.client.query("BEGIN")
        .then(() => done(null, self))
        .catch(done)
})

const _commit = _.promise((self, done) => {
    self.postgres.client.query("COMMIT")
        .then(() => done(null, self))
        .catch(done)
})

const _rollback = _.promise((self, done) => {
    self.postgres.client.query("ROLLBACK")
        .then(() => done(null, self))
        .catch(done)
})


/*
 *  Requires: self.postgres
 *  Parameters: paramd.method or method
 *
 *  The "method" (a promise) is called inside 
 *  a Postgres transaction. If there is an 
 *  exception inside the method, ROLLBACK is 
 *  done before the exception is passed on
 */
const transaction = _method => {
    const postgres = require("..")

    const f = _.promise((self, done) => {
        _.promise.validate(self, f)

        _.promise(self)
            .add("rollback", false)
            .then(_begin)

            .then(_method)

            .except(error => {
                if (!error.self) {
                    throw error
                }

                error.self.rollback = true
                error.self.rollback_error = error

                return error.self
            })
            .conditional(
                sd => sd.rollback,
                _rollback,
                _commit
            )
            .conditional(
                sd => sd.rollback_error,
                _.promise(sd => {
                    throw sd.rollback_error
                })
            )
            .end(done)
    })

    f.method = "transaction()"
    f.description = ``
    f.requires = {
        postgres: _.is.Object,
    }
    f.accepts = {
    }
    f.produces = {
    }

    return f
}


/**
 *  API
 */
exports.transaction = transaction
