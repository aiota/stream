var aiota = require("aiota-utils");
var path = require("path");
var http = require("http");
var url = require("url");
var amqprpc = require("amqp-rpc");
var MongoClient = require("mongodb").MongoClient;

var config = null;
//var db = null;

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

				var rpc = amqprpc.factory({ url: "amqp://" + config.amqp.login + ":" + config.amqp.password + "@" + config.amqp.host + ":" + config.amqp.port });
/*
				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, dbConnection) {
					if (err) {
						aiota.log(path.basename(__filename), config.server, aiotaDB, err);
					}
					else {
						db = dbConnection;
*/						
						var port = config.ports["aiota-stream"][0];
							 
						http.createServer(function (request, response) {
							var queryData = url.parse(request.url, true).query;

							if (queryData.hasOwnProperty("deviceId") && queryData.hasOwnProperty("tokencardId") {
								response.writeHead(200, {
									"Content-Type": "text/event-stream",
									"Cache-Control": "no-cache",
									"Access-Control-Allow-Origin": "*"
								});
							
								var obj = {
									header: {
										requestId: "testReqId",
										deviceId: queryData.deviceId,
										type: "poll",
										timestamp: Date.now(),
										ttl: 86400,
										encryption: {
											method: "aes-256-gcm",
											tokencardId: queryData.tokencardId
										}
									},
									body: { timeout: 0 },
									nonce: 0
								};

								rpc.call("longpolling-queue", obj, function(result) {
									console.log(result);
									response.write("data: " + JSON.stringify(result) + "\n\n");
								});

/*
								db.collection("applications", function(err, collection) {
									if (err) {
//										callback({ error: err, errorCode: 200001 });
										return;
									}
									
									collection.findOne({ _id: queryData.tokencardId }, { _id: 0, tokens: 1 }, function(err, appl) {
										if (err) {
//											callback({ error: err, errorCode: 200002 });
											return;
										}
										else {
											if (appl) {
												if (appl.hasOwnProperty("tokens")) {
													db.collection("push_actions", function(err, collection) {
														if (err) {
//															callback({ error: err, errorCode: 200001 });
															return;
														}
								
														// First we insert a dummy push action to guarantee that the tailable cursor returns a valid cursor
														collection.insert({ deviceId: queryData.deviceId, encryption: { method: "none", tokencardId: queryData.tokencardId }, status: 0, timeoutAt: 0 }, function(err, result) {
															if (err) {
//																callback({ error: err, errorCode: 200002 });
																return;
															}
											
															// Now create a tailable cursor for this device				
															var filter = { deviceId: queryData.deviceId, "encryption.tokencardId": queryData.tokencardId, status: { $lt: 10 } };
															
															// Set MongoDB cursor options
															var cursorOptions = {
																tailable: true,
																awaitdata: true,
																numberOfRetries: -1
															};
															
															// Create stream and listen
															var stream = collection.find(filter, cursorOptions).stream();
																	
															// call the callback
															stream.on("data", function(doc) {
																var now = Date.now();
																var sendToDevice = true;
																
																if (doc.timeoutAt <= now) {
																	// This action has timed out
																	// actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "timed out" }, status: 30 });
																	sendToDevice = false;
																}
																else {
																	if (doc.status == 0) {
																		//actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "sent to device" }, status: 1, resendAfter: now + actions[i].resends.resendTimeout });
																	}
																	else {
																		// This is a resend
																		if (doc.resends.numResends < doc.resends.maxResends) {
																			if (doc.resends.resendAfter <= now) {
																				// The resend timeout has expired
																				//actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "resent to device" }, status: 2, resendAfter: now + actions[i].resends.resendTimeout });
																			}
																			else {
																				// Don't send this action until the resend timeout expires
																				sendToDevice = false;
																			}
																		}
																		else {
																			// We have exhausted the maximum number of resends
																			//actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "max. resends exhausted" }, status: 31 });
																			sendToDevice = false;
																		}
																	}
																}
											
																if (sendToDevice) {
																	var action = { action: doc.action, requestId: doc.requestId };
																	if (doc.hasOwnProperty("params")) {
																		action["params"] = doc.params;
																	}
																	
																	var nonce = 0;
						
																	aiota.respond(null, queryData.deviceId, { group: "response", type: "stream" }, doc.encryption.method, queryData.tokencardId, appl.tokens, nonce, action, function(msg) {
																		response.write("data: " + JSON.stringify(msg) + "\n\n");
																	});
						
																	//process.nextTick(updateActions);		
																}
															});												
															});
													});
												}
												else {
	//												callback({ error: "The application tokens are not defined.", errorCode: 100023 });
												}
											}
											else {
	//											callback({ error: "The application is not defined.", errorCode: 100016 });
											}
										}
									});
								});
*/								
								response.on("close", function () {
								});
							}
						}).listen(port);

						setInterval(function() { aiota.heartbeat(path.basename(__filename), config.server, aiotaDB); }, 10000);
		
						process.on("SIGTERM", function() {
							aiota.terminateProcess(path.basename(__filename), config.server, aiotaDB, function() {
								process.exit(1);
							});
						});
/*
					}
				});
*/
			}
		});
	}
});