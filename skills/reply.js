var RtmClient = require('@slack/client').RtmClient;
var RTM_EVENTS = require('@slack/client').RTM_EVENTS;
var APIAI_SERVICE = require('./apiai/apiaiService.js');

var bot_token = process.env.npm_package_config_SLACK_BOT_TOKEN || '';
var rtm = new RtmClient(bot_token);
rtm.start();

var successCallback = function(response,message){
    rtm.sendMessage(response.result.fulfillment.speech, message.channel);
}

var errorCallback = function(error,message){
    rtm.sendMessage(error, message.channel);
}

rtm.on(RTM_EVENTS.MESSAGE, APIAI_SERVICE.getResponse.bind(this, rtm, successCallback, errorCallback));
