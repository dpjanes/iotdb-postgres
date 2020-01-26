/*
 *  execute.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-01-24
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

/*
 */
const execute = _.promise((self, done) => {
    _.promise.validate(self, execute)

    if (self.verbose) {
        console.log("-", execute.method, self.statement);
        (self.params || []).forEach(param => {
            console.log(" ", param)
        })
    }

    self.postgres.client.query(self.statement, self.params)
        .then(result => {
            self.postgres$result = result

            let last_result = result
            if (_.is.Array(result)) {
                last_result = result[result.length - 1]

            }
            self.jsons = last_result.rows.map(d => Object.assign({}, d))
            self.count = last_result.rowCount
            self.json = self.jsons.length ? self.jsons[0] : null

            done(null, self)
        })
        .catch(error => {
            if (self.verbose) {
                console.log("#", execute.method, _.error.message(error))
            }

            throw error
        })
        .catch(done)
})

execute.method = "execute"
execute.description = `
    Execute a Postgres statement. The rows coming back are
    not pure Dictionaries so we do a little munging.`
execute.requires = {
    postgres: {
        client: _.is.Object,
    },
    statement: _.is.String,
}
execute.accepts = {
    params: _.is.Array,
}
execute.produces = {
    postgres$result: _.is.Object,
    jsons: _.is.Array,
    json: _.is.Dictionary,
    count: _.is.Integer,
}
execute.params = {
    statement: _.p.asis,
    params: _.p.asis,
}
execute.p = _.p(execute)

/**
 *  API
 */
exports.execute = execute
