# Jarvis

![N|Solid](https://depts.washington.edu/nwcenter/wp-content/uploads/2016/09/maxresdefault.jpg)
Jarvis is a slack bot that uses api.ai for chattiness.

This is skeleton code to get a node.js based chatty slack bot up by supplying SLACK bot token and API.AI agent client token in package.json

  - slack bot token - https://slackapi.github.io/node-slack-sdk/bots.html
  - api.ai agent client token - https://docs.api.ai/docs/authentication

### Tech

Jarvis uses a number of open source projects to work properly:

* [node.js] - evented I/O for the backend
* [node-slack-sdk] - https://slackapi.github.io/node-slack-sdk/
* [node-apiai-sdk] - https://www.npmjs.com/package/apiai

### Installation

Jarvis requires [Node.js](https://nodejs.org/) v4+ to run.

Install the dependencies start the chatty bot.

```sh
$ cd jarvis
$ npm install
$ npm run reply
```
