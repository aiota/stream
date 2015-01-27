var aiota = require("aiota-utils");
var path = require("path");
var http = require("http");
var url = require("url");
var amqp = require("amqp");
var amqprpc = require("amqp-rpc");
var MongoClient = require("mongodb").MongoClient;
var ws = require("ws").Server;

var config = null;
var rpc = null;
var buffer = false;
var bus = null;
var app = null;

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
		var connection = null;
		
		for (var i = 0; i < openConnections.length; ++j) {
			if ((openConnections[i].deviceId == deviceId) && (openConnections[i].tokencardId == tokencardId)) {
				connection = openConnections[i].connection;
				break;
			}
		}
		
		if (connection) {
			switch (connection.type) {
			case "sse":			connection.response.write("data: " + JSON.stringify(result) + "\n\n");
								break;
			case "websocket":	connection.response.send(JSON.stringify(result));
								break;
			}
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

var processRequest = function(request, response) {
	var queryData = url.parse(request.url, true).query;

	if (queryData.hasOwnProperty("deviceId") && queryData.hasOwnProperty("tokencardId")) {
		response.writeHead(200, {
			"Content-Type": "text/event-stream",
			"Cache-Control": "no-cache",
			"Access-Control-Allow-Origin": "*"
		});
	
		openConnections.push({ deviceId: queryData.deviceId, tokencardId: queryData.tokencardId, connection: { response: response, type: "sse" } });

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
};

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

				bus = amqp.createConnection(config.amqp);
				
				bus.on("ready", function() {
					var port = config.ports["aiota-stream"][0];
						 
					app = http.createServer(processRequest).listen(port);

					var wss = new ws({ server: app });
					
					wss.on("connection", function(wsConnect) {
						wsConnect.on("message", function(msg) {
							var obj = JSON.parse(msg);
							
							openConnections.push({ deviceId: obj.deviceId, tokencardId: obj.tokencardId, connection: { response: wsConnect, type: "websocket" } });
					
							bus.queue("push:" + obj.deviceId + "@" + obj.tokencardId, { autoDelete: true, durable: false }, function(queue) {
								queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
									if (getConnectionIndex(obj.deviceId, obj.tokencardId) < 0) {
										queue.destroy();
									}
									else {
										constructSSE(obj.deviceId, obj.tokencardId);
										queue.shift();
									}
								});
							});
							
							constructSSE(obj.deviceId, obj.tokencardId);
						});
					});
				});
			}
		});
	}
});