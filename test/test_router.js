"use strict";

require('core-js/shim');

/*
*  test router to exercise all tests uning the restbase-cassandra handler
*/
var fs = require('fs');
var yaml = require('js-yaml');

var RouteSwitch = require('routeswitch');

function setupConfigDefaults(conf) {
    if (!conf) {
        conf = yaml.safeLoad(
                fs.readFileSync(__dirname + '/test_router.conf.yaml')) ;
    }
    return conf;
}

var router = {};
router.request = function(req) {
    var match = this.newRouter.match(req.uri);
    if (match) {
        req.params = match.params;
        var handler = match.methods[req.method.toLowerCase()];
        if (handler) {
            return handler({}, req)
            .then(function(item){
                return item;
            });
        } else {
            throw new Error('No handler for ' + req.method + ' ' + req.uri);
        }
    } else {
        throw new Error('No match for ' + req.method + ' ' + req.uri);
    }
};

function flatHandlerFromModDef (modDef, prefix) {
    var handler = { paths: {} };
    Object.keys(modDef.spec.paths).forEach(function(path) {
        var pathModSpec = modDef.spec.paths[path];
        handler.paths[prefix + path] = {};
        Object.keys(pathModSpec).forEach(function(m) {
            var opId = pathModSpec[m].operationId;
            if (!modDef.operations[opId]) {
                throw new Error('The module does not export the opration ' + opId);
            }
            handler.paths[prefix + path][m] = modDef.operations[opId];
        });
    });
    return handler;
}

router.makeRouter = function(req) {
    var self = this;
    var conf = setupConfigDefaults();
    var opt = {
        log: function(){},
        conf: conf
    };

    return require('../index.js')(opt)
    .then(function(modDef) {
        self.newRouter = new RouteSwitch.fromHandlers([flatHandlerFromModDef(modDef, '/{domain}/sys/table')]);
        return self;
    })
    .catch(function(e) {
        console.log('Error during RESTBase startup: ', e);
    });
};

module.exports = router;
