var RtmClient = require('@slack/client').RtmClient;
var RTM_EVENTS = require('@slack/client').RTM_EVENTS;
var bot_token = process.env.npm_package_config_SLACK_BOT_TOKEN || '';
console.log(process.env.SLACK_BOT_TOKEN);
var rtm = new RtmClient(bot_token);

rtm.on(RTM_EVENTS.MESSAGE, function handleRtmMessage(message) {
  console.log('Message:', message); //this is no doubt the lamest possible message handler, but you get the idea
});

rtm.start();