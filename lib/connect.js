/*
 *  connect.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-04-01
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

/**
 *  Requires: self.postgres, self.postgres_url
 */
const connect = _.promise((self, done) => {
    _.promise.validate(self, connect)

    self.postgres = {
        clients: {},
        client: null,
        postgresd: self.postgresd,
    }

    const client = self.postgres.clients[self.postgres_url]
    if (client) {
        self.postgres.client = client

        return done(null, self)
    }

    self.postgres.client = new postgres.Client(self.postgres_url)
    self.postgres.client.connect(error => {
        if (error) {
            return done(error)
        }

        self.postgres.clients[self.postgres_url] = self.postgres.client

        done(null, self)
    })
})

connect.method = "connect"
connect.description = ``
connect.requires = {
    postgres: _.is.Object,
    postgres_url: _.is.String,
}
connect.accepts = {
}
connect.produces = {
}
connect.params = {
}
connect.p = _.p(connect)

/**
 *  API
 */
// exports.connect = connect
