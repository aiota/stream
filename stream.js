var aiota = require("aiota-utils");
var path = require("path");
var http = require("http");
var url = require("url");
var amqp = require("amqp");
var amqprpc = require("amqp-rpc");
var MongoClient = require("mongodb").MongoClient;

var config = null;
var rpc = null;
var buffer = false;

var openConnections = [];

function longpollingRequest(deviceId, tokencardId, callback)
{
	if (rpc == null) {
		callback([]);
		return;
	}

	var obj = {
		header: {
			requestId: "streamReq",
			deviceId: deviceId,
			type: "poll",
			timestamp: Date.now(),
			ttl: 86400,
			encryption: {
				method: "aes-256-gcm",
				tokencardId: tokencardId
			}
		},
		body: { timeout: 0 },
		nonce: 0
	};

	rpc.call("longpolling-queue", obj, function(result) {
		callback(result);
	});
}

function constructSSE(deviceId, tokencardId)
{
	longpollingRequest(deviceId, tokencardId, function(result) {
		var d = new Date();
		var response = null;
		
		for (var i = 0; i < openConnections.length; ++j) {
			if ((openConnections[i].deviceId == deviceId) && (openConnections[i].tokencardId == tokencardId)) {
				response = openConnections[i].response;
				break;
			}
		}
		
		if (response) {
 			response.write("data: " + JSON.stringify(result) + "\n\n");
		}
	});
}

function getConnectionIndex(deviceId, tokencardId)
{
	var index = -1;

	for (var j = 0; j < openConnections.length; ++j) {
		if ((openConnections[j].deviceId == deviceId) && (openConnections[j].tokencardId == tokencardId)) {
			index = j;
			break;
		}
	}
	
	return index;
}

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, aiotaDB) {
	if (err) {
		aiota.log(path.basename(__filename), "", null, err);
	}
	else {
		aiota.getConfig(aiotaDB, function(c) {
			if (c == null) {
				aiota.log(path.basename(__filename), "", aiotaDB, "Error getting config from database");
			}
			else {
				config = c;

				rpc = amqprpc.factory({ url: "amqp://" + config.amqp.login + ":" + config.amqp.password + "@" + config.amqp.host + ":" + config.amqp.port });

				var bus = amqp.createConnection(config.amqp);
				
				bus.on("ready", function() {
					var port = config.ports["aiota-stream"][0];
						 
					http.createServer(function (request, response) {
						var queryData = url.parse(request.url, true).query;
	
						if (queryData.hasOwnProperty("deviceId") && queryData.hasOwnProperty("tokencardId")) {
							response.writeHead(200, {
								"Content-Type": "text/event-stream",
								"Cache-Control": "no-cache",
								"Access-Control-Allow-Origin": "*"
							});
						
							openConnections.push({ deviceId: queryData.deviceId, tokencardId: queryData.tokencardId, response: response });
 	
							bus.queue("push:" + queryData.deviceId + "@" + queryData.tokencardId, { autoDelete: true, durable: false }, function(queue) {
								queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
									if (getConnectionIndex(queryData.deviceId, queryData.tokencardId) < 0) {
										queue.destroy();
									}
									else {
										constructSSE(queryData.deviceId, queryData.tokencardId);
										queue.shift();
									}
								});
							});
							
							constructSSE(queryData.deviceId, queryData.tokencardId);
						}
						
						response.on("close", function() {
							var toRemove = getConnectionIndex(queryData.deviceId, queryData.tokencardId);
						
							if (toRemove >= 0) {
								openConnections.splice(toRemove, 1);
							}
						});
					}).listen(port);
	
/*
					setInterval(function() { aiota.heartbeat(path.basename(__filename), config.server, aiotaDB); }, 10000);
	
					process.on("SIGTERM", function() {
						aiota.terminateProcess(path.basename(__filename), config.server, aiotaDB, function() {
							process.exit(1);
						});
					});
*/
				});
			}
		});
	}
});