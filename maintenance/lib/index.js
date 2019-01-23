'use strict';

var fs   = require('fs');
var yaml = require('js-yaml');

/**
 * Return the table section of a RESTBase config.
 *
 * @param  {string}  config  - Path to a RESTBase YAML configuration file.
 * @return {Object}  table section of configuration.
 */
function getConfig(config) {
    // Read a RESTBase configuration from a (optional) path argument, an (optional) CONFIG
    // env var, or from /etc/restbase/config.yaml
    var conf;

    if (config) {
        conf = config;
    } else if (process.env.CONFIG) {
        conf = process.env.CONFIG;
    } else {
        conf = '/etc/restbase/config.yaml';
    }

    var confObj = yaml.safeLoad(fs.readFileSync(conf));
    return confObj.default_project['x-modules'][0].options.table;
}

module.exports = {
    getConfig
};
