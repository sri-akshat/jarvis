var apiai = require('apiai');
// create an agent on console.api.ai, put the client key below
var AGENT_TOKEN = process.env.npm_package_config_APIAI_AGENT_TOKEN || '';
var app = apiai(AGENT_TOKEN);

var getResponse = function ( rtm, successCallback, errorCallback, message) {
    var request = app.textRequest(message.text, {
        sessionId: '123'
    });

    request.on('response', function(response) {
    	console.log(response);
        successCallback(response, message);
    });

    request.on('error', function(error) {
        errorCallback(error, message);
    });
    request.end();
}

module.exports.getResponse = getResponse;
