#!/usr/bin/env node
"use strict";


global.Promise = require('bluebird');

var cass = require('cassandra-driver');
var makeClient = require('../lib/index');
var router = require('../test/test_router.js');
var DB = require('../lib/db.js');
var dbu = require('../lib/dbutils.js');
var fs = require('fs');
var util = require('util');


function usage(exit_code) { 
    var node_bin = process.argv[0];
    var script_bin = process.argv[1];
    console.log("Usage: %s %s [options] <api-path>", node_bin, script_bin);
    console.log("  options:");
    console.log("    -m <method>  the method to use, default: get");
    console.log("    -d <data>    the data to use as the request body");
    console.log("    -f <fname>   load data from file <fname>");
    console.log("    -j           interpret data as JSON");
    console.log("    -h           print this help and exit");
    console.log("  <api-path>     the path to route the request to");
    if (typeof exit_code === 'undefined')
        exit_code = 1;
    process.exit(exit_code);
}


function parse_data(data_str) {
    var ret;
    try {
        ret = JSON.parse(data_str);
    } catch(err) {
        try {
            ret = eval('(' + data_str + ')');
        } catch(eval_err) {
            console.log("Error while parsing input data: %s", eval_err.message);
            process.exit(2);
        }
    }
    return ret;
}


var args = process.argv.slice(2);
if (args == null || args.length == 0) {
    usage();
}

var opts = {
    path: null,
    method: 'get',
    data: null,
    is_json: false
};
var exp_method = false;
var exp_data = false;
var exp_fname = false;
args.forEach(function(arg, index, array) {
    switch(arg) {
        case '-h':
            usage();
        case '-m':
            exp_method = true;
            break;
        case '-d':
            exp_data = true;
            break;
        case '-f':
            exp_fname = true;
            break;
        case '-j':
            opts.is_json = true;
            if (typeof opts.data === String && opts.data.length) {
                opts.data = parse_data(opts.data);
            }
            break;
        default:
            if (exp_method) {
                opts.method = arg.toLowerCase();
                exp_method = false;
            } else if (exp_data) {
                opts.data = opts.is_json ? parse_data( arg ) : arg;
                exp_data = false;
            } else if (exp_fname) {
                var data = fs.readFileSync(arg, {encoding: 'utf8'});
                opts.data = opts.is_json ? parse_data( data ) : data;
                exp_fname = false;
            } else {
                if (arg[0] == '/') {
                    opts.path = arg;
                } else {
                    opts.path = '/' + arg;
                }
            }
    }
});

if (!opts.path || !opts.path.length) {
    console.log("The path is obligatory!");
    usage();
}


makeClient({
    log: console.log,
    conf: {
        hosts: ['localhost']
    }
})
.then(function(db) {
    DB = db;
    return router.makeRouter();
}).then(function(r_obj) {
    var req = {
        url: opts.path,
        method: opts.method
    };
    if (opts.data !== null) {
        req.body = opts.data;
    }
    console.log("#~> REQ : %s", util.inspect(req));
    return r_obj.request(req);
}).then(function(response) {
    console.log("#~> RESP: %s", util.inspect(response));
    process.exit();
}).catch(function(err) {
    console.log("#~> ERR : %s", util.inspect(err));
});

