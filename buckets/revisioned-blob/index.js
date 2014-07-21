"use strict";

/**
 * Revisioned blob handler
 */

var RevisionBackend = require('./cassandra');
var RouteSwitch = require('routeswitch');

var backend;
var config;

function RevisionedBlob (backend) {
    this.store = new RevisionBackend(backend);
}

var revisionSwitch = new RouteSwitch([{
    pattern: '/{title}/rev/{rev}/{prop}'
}]);

RevisionedBlob.prototype.handlePOST = function (env, req) {
    var match = revisionSwitch.match(req.uri);

    var title = match.params.title;
    if (title !== undefined) {
        // Atomically create a new revision with several properties
        if (req.body._rev && req.body._timestamp) {
            var props = {};
            props[match.params.prop] = {
                value: new Buffer(req.body[match.params.prop])
            };
            //console.log(props);
            var revision = {
                page: {
                    title: title
                },
                id: Number(req.body._rev),
                timestamp: req.body._timestamp,
                props: props
            };
            return this.store.addRevision(revision)
            .then(function (result) {
                return {
                    status: 200,
                    body: {'message': 'Added revision ' + result.tid, id: result.tid}
                };
            })
            .catch(function(err) {
                // XXX: figure out whether this was a user or system
                // error
                //console.error('Internal error', err.toString(), err.stack);
                return {
                    status: 500,
                    body: err.toString()
                };
            });
        } else {
            // We don't support _rev or _timestamp-less revisions yet
            return Promise.resolve({
                status: 400,
                body: '_rev or _timestamp are missing!'
            });
        }
    } else {
        console.log(title, req.params, match.params);
        return Promise.resolve({
            status: 404,
            body: 'Not found'
        });
    }
};


RevisionedBlob.prototype.handleGET = function (env, req) {
    var match = revisionSwitch.match(req.uri);

    var page = match.params.title;
    if (page && match.params.rev) {
        var revString = match.params.rev,
            // sanitized / parsed rev
            rev = null,
            // 'wikitext', 'html' etc
            prop = 'wikitext'; //queryComponents[2] || null;

        if (revString === 'latest') {
            // latest revision
            rev = revString;
        } else if (/^\d+$/.test(revString)) {
            // oldid
            rev = Number(revString);
        } else if (/^\d{4}-\d{2}-\d{2}/.test(revString)) {
            // timestamp
            rev = new Date(revString);
            if (isNaN(rev.valueOf())) {
                // invalid date
                return Promise.resolve({
                    status: 400,
                    body: 'Invalid date'
                });
            }
        } else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(revString)) {
            // uuid
            rev = revString;
        }

        if (page && prop && rev) {
            //console.log(page, prop, rev);
            return this.store.getRevision(page, rev, prop)
            .then(function (results) {
                if (!results.length) {
                    return {
                        status: 404,
                        body: 'Not found'
                    };
                }
                return {
                    status: 200,
                    headers: {'Content-type': 'text/plain'},
                    body: results[0].value
                };
            })
            .catch(function(err) {
                    //console.error('ERROR', err.toString(), err.stack);
                    return {
                        status: 500,
                        body: 'Internal error'
                    };
            });
        }
    }

    return Promise.resolve({
        status: 404,
        body: 'Not found'
    });
};

module.exports = function(options) {
    var revBlob = new RevisionedBlob(options.backend);
    return {
        //create: createBucket,
        //delete: deleteBucket,
        verbs: {
            GET: revBlob.handleGET.bind(revBlob),
            POST: revBlob.handlePOST.bind(revBlob)
        }
    };
};
