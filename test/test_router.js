"use strict";

/*
*  test router to exercise all tests uning the restbase-cassandra handler
*/

var RouteSwitch = require('routeswitch');

function setupConfigDefaults(conf) {
    if (!conf) { conf = {
            // module name
            type: "restbase-cassandra",
            hosts: ["localhost"],
            keyspace: "system",
            username: "cassandra",
            password: "cassandra",
            defaultConsistency: 'one'
        };
    }
    return conf;
}

var router = {};
router.request = function(req) {
	var match = this.newRouter.match(req.url);
	if (match) {
		req.params = match.params;
		var handler = match.methods[req.method.toLowerCase()];
		if (handler && handler.request_handler) {
			return handler.request_handler({}, req)
			.then(function(item){
				return item;
			});
		}
	}
};

router.makeRouter = function(req) {
	var self = this;
	var conf = setupConfigDefaults();
	var opt = {
		log: function(){},
		conf: conf
	};

	return require('../index.js')(opt)
	.then(function(handler) {
		self.newRouter = new RouteSwitch.fromHandlers([handler]);
		return self;
	})
	.catch(function(e) {
		console.log('Error during RESTBase startup: ', e);
	});
};

module.exports = router;
