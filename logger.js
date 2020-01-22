/*
 *  logger.js
 *
 *  David Janes
 *  IOTDB.org
 *  2018-08-25
 *
 *  Copyright (2013-2020) David P. Janes
 */

"use strict"

const _ = require("iotdb-helpers")

const path = require("path")

const _root = path.dirname(path.dirname(__filename));

const logger = (source) => _.logger.make({
    name: "iotdb-postgres",
    source: source.substring(_root.length + 1).replace(/[.]js$/, ""),
})

/**
 *  API
 */
module.exports = logger;
