var aiota = require("aiota-utils");
var http = require("http");
var MongoClient = require("mongodb").MongoClient;

var config = null;
var processName = "stream.js";
var db = null;

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, aiotaDB) {
	if (err) {
		aiota.log(processName, "", null, err);
	}
	else {
		aiota.getConfig(aiotaDB, function(c) {
			if (c == null) {
				aiota.log(processName, "", aiotaDB, "Error getting config from database");
			}
			else {
				config = c;

				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, dbConnection) {
					if (err) {
						aiota.log(processName, config.serverName, aiotaDB, err);
					}
					else {
						db = dbConnection;
						
						http.createServer(function (request, response) {
							response.writeHead(200, {
								"Content-Type": "text/event-stream",
								"Cache-Control": "no-cache",
								"Access-Control-Allow-Origin": "*"
							});
						
							db.collection("push_actions", function(err, collection) {
								if (err) {
									return;
								}
		
								var filter = {};
								
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
									response.write("data: " + JSON.stringify(doc) + "\n\n");
								});
							});
							
							response.on("close", function () {
							});
						}).listen(port);
					}
				});
			}
		});
	}
});