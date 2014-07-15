"use strict";

/**
 * Revisioned blob handler
 */

function handlePOST (req) {
	var title = req.params.title;
	if (req.query['rev/'] !== undefined) {
		// Atomically create a new revision with several properties
		if (req.body._rev && req.body._timestamp) {
			var revision = {
					page: {
						title: title
					},
					id: Number(req.body._rev),
					timestamp: req.body._timestamp,
					props: {
						wikitext: {
							value: new Buffer(req.body.wikitext)
						}
					}
				},
				store = getHandler(req.params.user, req.params.bucket);
			if (!store) {
				return next(new restify.ResourceNotFoundError('Store not found.'));
			}
			store.addRevision(revision,
				function (err, result) {
					if (err) {
						// XXX: figure out whether this was a user or system
						// error
						//console.error('Internal error', err.toString(), err.stack);
						return next(new restify.InternalError(err.toString()));
					}
					res.json({'message': 'Added revision ' + result.tid, id: result.tid});
					return next();
				});
		} else {
			// We don't support _rev or _timestamp-less revisions yet
			return next(new restify.MissingParameterError('_rev or _timestamp are missing!'));
		}
	} else {
	console.log(title, req.params);
		return next(new restify.ResourceNotFoundError());
	}
}


function handleGET (req, res, next) {
	var queryKeys = Object.keys(req.query);

	// First some rudimentary input validation
	if (queryKeys.length !== 1) {
		return next(new restify.MissingParameterError('Exactly one query parameter expected.'));
	}
	var store = getHandler(req.params.user, req.params.bucket);
	if (!store) {
		return next(new restify.ResourceNotFoundError());
	}

	var page = req.params.title,
		query = queryKeys[0],
		queryComponents = query.split(/\//g);
	if (/^rev\//.test(query)) {
		if (queryComponents.length >= 2) {
			var revString = queryComponents[1],
				// sanitized / parsed rev
				rev = null,
				// 'wikitext', 'html' etc
				prop = queryComponents[2] || null;

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
					return next(new restify.InvalidArgumentError('Invalid date'));
				}
			} else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(revString)) {
				// uuid
				rev = revString;
			}

			if (page && prop && rev) {
				//console.log(query);
				store.getRevision(page, rev, prop, function (err, results) {
					if (err) {
						//console.error('ERROR', err.toString(), err.stack);
						return next(new restify.InternalError(err.toString()));
					}
					if (!results.length) {
						return next(new restify.ResourceNotFoundError());
					}
					res.writeHead(200, {'Content-type': 'text/plain'});
					res.end(results[0][0]);
				});
				return;
			}
		}
	}

	return next(new restify.ResourceNotFoundError());
}

modules.export = {
    //create: createBucket,
    //delete: deleteBucket,
    verbs: {
        get: handleGET,
        post: handlePOST
    }
};
