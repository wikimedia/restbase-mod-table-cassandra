if (!global.Promise) {
    global.Promise = require('bluebird');
}
if (!Promise.promisify) {
    Promise.promisify = require('bluebird').promisify;
}

require('../storage/cassandra/test.js');
