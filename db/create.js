/*
 *  db/create.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-01-25
 *
 *  Copyright (2013-2020) David P. Janes
 *
 *  Licensed under the Apache License, Version 2.0 (the "License")
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

const logger = require("../logger")(__filename)

/**
 */
const _normalize = column => {
    const c = {
        column_name: column.name,
        data_type: column.type.replace(/ .*$/, "").toLowerCase(),
        character_maximum_length: null,
        _name: column.name,
        _type: column.type.replace(/ .*$/, ""),
        _ignore: false,
    }

    const t = column.type.toLowerCase()
    if (t === "serial") {
        c.data_type = "integer"
        c._ignore = true
    } else if (t === "double precision") {
        c._type = "double precision" // because of the space hack above
    } else if (t.match(/^varchar\((\d+)\)/)) {
        c.data_type = "character varying"
        c.character_maximum_length = parseInt(t.match(/^varchar\((\d+)\)/)[1])
    }

    return c
}

/**
 */
const _fix = _.promise((self, done) => {
    _.promise.validate(self, _fix)

    self.postgres.client.query(`
select column_name, data_type, character_maximum_length
from INFORMATION_SCHEMA.COLUMNS where table_name = '${self.table_schema.name}';
`)
        .then(result => {
            self.rowd = {}

            result.rows.forEach(row => {
                self.rowd[row.column_name] = Object.assign({}, row)
            })

            let columns = self.table_schema.columns.map(_normalize);
            columns.forEach(column => {
                /* if (self.table_schema.keys.indexOf(column._name) > -1) {
                    column._action = null
                } else */ if (column._ignore) {
                    column._action = null
                } else if (self.rowd[column.column_name]) {
                    column._action = `ALTER ${column._name} TYPE ${column._type}`
                } else {
                    column._action = `ADD ${column._name} ${column._type}`
                }
            })

            // console.log("ROWD", result.rows)
            // console.log("COLUMNS", columns)

            columns = columns.filter(column => column._action)
            if (columns.length === 0) {
                return done(null, self)
            }
            
            const statement = `ALTER TABLE ${self.table_schema.name}\n\t${columns.map(c => c._action).join(",\n\t")}`
            logger.debug({
                method: _fix.method,
                statement: statement,
            }, "upgrading existing table")

            self.postgres.client.query(statement)
                .then(result => {
                    self.postgres_result = result;
                    done(null, self)
                })
                .catch(done)
        })
        .catch(done)
})

_fix.method = "db.create/_fix"
_fix.requires = {
    postgres: {
        client: _.is.Object,
    },
    table_schema: {
        name: _.is.String,
        keys: _.is.Array,
    },
}

/**
 */
const _create_index = _.promise((self, done) => {
    _.promise.validate(self, _create_index)

    const statement = `CREATE INDEX ${self.id.name} ON ${self.table_schema.name} (${self.id.columns.join(",")})`

    self.postgres.client.query(statement)
        .then(result => {
            done(null, self)
        })
        .catch(error => {
            if (error.code === '42P07') {
                return done(null, self)
            } else {
                return done(error)
            }
        })
})

_create_index.method = "db._create_index/_create_index"
_create_index.requires = {
    postgres: {
        client: _.is.Object,
    },
    table_schema: {
        name: _.is.String,
        keys: _.is.Array,
    },
}

/*
 */
const create = dry_run => _.promise((self, done) => {
    _.promise.validate(self, create)

    const columns = (self.table_schema.columns || []).map(cd => `${cd.name} ${cd.type}`);

    columns.push(`PRIMARY KEY (${self.table_schema.keys.join(", ")})`)

    const statement = `CREATE TABLE ${self.table_schema.name}(${columns.join(", ")})`;

    self.postgres_statement = statement
    self.postgres_params = null
    self.postgres_result = null

    if (dry_run) {
        return done(null, self)
    }

    const _create_table = _.promise((self, done) => {
        self.postgres.client.query(statement)
            .then(result => {
                self.postgres_result = result;
                done(null, self)
            })
            .catch(done)
    })

    const ids = [];
    _.mapObject(self.table_schema.indexes || {}, (columns, name) => {
        ids.push({
            name: name,
            columns: columns,
        })
    })

    _.promise(self)
        .then(_create_table)
        .then(_.promise.bail)
        .except(error => {
            if (error.code !== "42P07") {
                throw error
            }

            return self
        })
        .then(_fix)
        .except(error => {
            if (error.code !== "0A000") {
                throw error
            }

            logger.warn({
                method: create.method,
                table: self.table_schema.name,
                error: {
                    code: error.code,
                    message: error.message,
                },
            }, "ignoring this error but you may have an issue!!")

            return self
        })

        .except(_.promise.unbail)
        .add("ids", ids)
        .each({
            method: _create_index,
            inputs: "ids:id",
        })

        .end(done, self, "postgres_result")
})

create.method = "db.create"
create.description = `Create a Postgres table from table_schema`
create.requires = {
    postgres: {
        client: _.is.Object,
    },
    table_schema: {
        name: _.is.String,
        keys: _.is.Array,
    },
}
create.accepts = {
}
create.produces = {
    postgres_result: _.is.Object,
}

/**
 *  API
 */
exports.create = create(false)
exports.create.dry_run = create(true)
