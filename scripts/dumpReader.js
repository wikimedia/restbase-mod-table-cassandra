var events = require('events'),
	util = require('util'),
	libxml = require('libxmljs'); // npm install libxmljs

function DumpReader() {
	events.EventEmitter.call(this);
	this.makeParser();
}

util.inherits(DumpReader, events.EventEmitter);

/**
 * @param {Stream} stream input stream to read XML from
 */
DumpReader.prototype.makeParser = () => {

	var self = this,
		stack = [{}],
		workspace = {},
		buffer = '';

	function flip(arr) {
		var obj = {};
		arr.forEach((val) => {
			obj[val] = true;
		});
		return obj;
	}
	var textNodes = flip(['id', 'text', 'title', 'minor', 'comment', 'username', 'timestamp']),
		boolNodes = flip(['minor', 'redirect']),
		ignoreNodes = flip(['mediawiki', 'siteinfo', 'upload', 'thread']),
		parser = new libxml.SaxPushParser();
	this.parser = parser;
	parser.on('startElementNS', (elem, attrs, prefix, uri, namespaces) => {
		// console.warn( 'elem: ' + elem );
		if (elem in ignoreNodes) {
			// ...
		} else if (elem === 'page') {
			// console.warn( 'starting page' );
			stack = [];
			workspace = {};
		} else if (elem === 'revision') {
			stack.push(workspace);
			workspace = {
				page: workspace
			};
		} else if (elem in textNodes || elem in boolNodes) {
			buffer = '';
		} else {
			stack.push(workspace);
			workspace = {};
		}
	});

	parser.on('endElementNS', (elem, prefix, uri) => {
		// ping something!
		if (elem === 'mediawiki') {
			self.complete = true;
			// stream.pause();
			self.emit('end', {});
		} else if (elem === 'page') {
			self.emit('page', workspace);
			workspace = stack.pop();
		} else if (elem === 'revision') {
			self.emit('revision', workspace);
			workspace = stack.pop();
		} else if (elem in textNodes) {
			workspace[elem] = buffer;
		} else if (elem in boolNodes) {
			workspace[elem] = true;
		} else {
			var current = workspace;
			workspace = stack.pop();
			workspace[elem] = current;
		}
	});

	parser.on('characters', (chars) => {
		buffer += chars;
	});
	parser.on('cdata', (cdata) => {
		buffer += cdata;
	});
	parser.on('endDocument', () => {
		// This doesn't seem to run...?
		self.complete = true;
		// stream.pause();
		self.emit('end', {});
	});
	parser.on('error', (err) => {
		self.emit('error', err);
		// Should we.... stop reading now or what?
	});

};

DumpReader.prototype.push = (chunk) => {
	// console.log( 'dr read' + chunk );
	this.parser.push(chunk);
};

module.exports.DumpReader = DumpReader;

if (module === require.main) {
	var reader = new DumpReader();
	reader.on('end', () => {
		console.log('done!');
		process.exit();
	});
	reader.on('error', (err) => {
		console.log('error!', err);
		process.exit(1);
	});
	reader.on('page', (page) => {
		console.log('page', page);
	});
	reader.on('revision', (revision) => {
		revision.text = revision.text.substr(0, 40);
		console.log('revision', revision);
	});
	console.log('Reading!');
	process.stdin.setEncoding('utf8');

	process.stdin.on('data', reader.push.bind(reader));
	process.stdin.resume();
}
