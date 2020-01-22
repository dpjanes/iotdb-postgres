/*
 *  initialize.js
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

const postgres = require("pg")

const assert = require("assert")

const logger = require("../logger")(__filename)

/**
 *  Requires: self.postgresd
 *  Produces: self.postgres
 */
const initialize = _.promise((self, done) => {
    const method = "initialize";

    assert.ok(self.postgresd, `${method}: expected self.postgresd`)

    self.postgres = self.postgres || {
        clients: {},
        client: null,
    }

    let connectd = null
    let client_key = null

    if (self.postgresd.url) {
        client_key = self.postgresd.url
        connectd = self.postgresd.url
    } else if (self.postgresd.user) {
        client_key = `${self.postgresd.user}@${self.postgresd.host}@${self.postgresd.database}`
        connectd = self.postgresd

        logger.debug({
            user: self.postgresd.user,
            host: self.postgresd.host,
            database: self.postgresd.database,
        }, "connected to postgres")
    } else {
        return done(null, self)
    }

    const client = self.postgres.clients[client_key]
    if (client) {
        self.postgres.client = client
        
        return done(null, self)
    }

    self.postgres.client = new postgres.Client(connectd)
    self.postgres.client.connect(error => {
        if (error) {
            return done(error);
        }

        self.postgres.clients[client_key] = self.postgres.client

        done(null, self)
    })
})

/**
 *  API
 */
exports.initialize = initialize;
exports.connect = initialize;
