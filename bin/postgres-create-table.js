/**
 *  bin/postgres-create-table
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

"use strict"

const _ = require("iotdb-helpers")
const fs = require("iotdb-fs")

const assert = require("assert")
const path = require("path")

const postgres = require("..")

const minimist = require('minimist')
const ad = minimist(process.argv.slice(2), {
    boolean: [ "verbose", "help", "drop", "error", ],
})

const help = message => {
    if (message) {
        console.log("postgres-create-table:", message)
        console.log()
    }
    console.log("usage: postgres-create-table [options] --db <url> <path>")
    console.log("")
    console.log("options:")
    console.log("  --db postgres://host:port/db  Postgres DB to connect to")
    console.log("  --drop                        Drop the existing database")

    process.exit(message ? 1 : 0)
}

if (ad.help) {
    help()
} else if (!ad.db) {
    help("--db postgres://host:port/db required")
} else if (ad._.length === 0) {
    help("<path> is required")
}

const _do_table = _.promise((self, done) => {
    assert.ok(self.path)
    assert.ok(self.postgres)

    _.promise(self)
        .then(fs.read.json)
        .then(_.promise.add("json:table_schema"))
        .then(_.promise.conditional(self.do_drop, _.promise.optional(postgres.db.drop)))
        .then(postgres.db.create)
        .then(_.promise(sd => {
            console.log("+", "made", sd.table_schema.name)
        }))
        .then(_.promise.done(done, self))
        .catch(done)
})

_.promise({
    postgresd: {
        url: ad.db,
    },
    verbose: ad.verbose,
    do_drop: ad.drop,
    paths: ad._,
})
    .then(postgres.initialize)
    .then(_.promise.series({
        method: _do_table,
        inputs: "paths:path",
    }))
    .catch(error => {
        delete error.self
        console.log("#", _.error.message(error))

        if (ad.error) {
            console.log(error)
        }
    })
    .done(() => {
        process.nextTick(() => process.exit())
    })
