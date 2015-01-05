var aiota = require("aiota-utils");
var path = require("path");
var http = require("http");
var url = require("url");
var MongoClient = require("mongodb").MongoClient;

var config = null;
var db = null;

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

				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, dbConnection) {
					if (err) {
						aiota.log(path.basename(__filename), config.server, aiotaDB, err);
					}
					else {
						db = dbConnection;
						
						var port = config.ports["aiota-stream"][0];
							 
						http.createServer(function (request, response) {
							var queryData = url.parse(request.url, true).query;

							if (queryData.hasOwnProperty("deviceId")) {
								response.writeHead(200, {
									"Content-Type": "text/event-stream",
									"Cache-Control": "no-cache",
									"Access-Control-Allow-Origin": "*"
								});
							
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
															return;
														}
								
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
																var action = { action: actions[i]["action"], requestId: actions[i]["requestId"] };
																if (actions[i].hasOwnProperty("params")) {
																	action["params"] = actions[i].params;
																}
																
																var nonce = 0;
					
																aiota.respond(null, queryData.deviceId, { group: "response", type: "stream" }, doc.encryptionMethod, queryData.tokencardId, appl.tokens, nonce, action, function(response) {
																	response.write("data: " + JSON.stringify(doc) + "\n\n");
																});
					
																//process.nextTick(updateActions);		
															}
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
								
								response.on("close", function () {
								});
							}
						}).listen(port);
					}
				});
			}
		});
	}
});