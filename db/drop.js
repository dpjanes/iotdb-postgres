/*
 *  db/drop.js
 *
 *  David Janes
 *  IOTDB.org
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

const postgres = require("pg");

const assert = require("assert");

/*
 *  Requires: self.postgres, self.table_schema
 *  Produces: self.postgres_result
 *
 *  Drop a Postgres Table
 */
const drop = _.promise((self, done) => {
    const method = "drop";

    assert.ok(self.postgres, `${method}: expected self.postgres`)
    assert.ok(self.postgres.client, `${method}: expected self.postgres.client`)
    assert.ok(self.table_schema, `${method}: expected self.table_schema`)
    assert.ok(self.table_schema.name, `${method}: expected self.table_schema.name`)

    const statement = `DROP TABLE ${self.table_schema.name}`;

    self.postgres.client.query(statement)
        .then(result => {
            self.postgres_result = result;

            done(null, self)
        })
        .catch(done)
})

/**
 *  API
 */
exports.drop = drop;
