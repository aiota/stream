var aiota = require("aiota-utils");
var http = require("http");
var MongoClient = require("mongodb").MongoClient;

var config = null;
var processName = "stream.js";
var db = null;

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, dbConnection) {
	if (err) {
		aiota.log(processName, "", null, err);
	}
	else {
		db = dbConnection;

		aiota.getConfig(db, function(c) {
			if (c == null) {
				aiota.log(processName, "", db, "Error getting config from database");
			}
			else {
				config = c;

				var port = config.ports["aiota-stream"][0];

				http.createServer(function (request, response) {
					response.writeHead(200, {
						"Content-Type": "text/event-stream",
						"Cache-Control": "no-cache",
						"Access-Control-Allow-Origin": "*"
					});
				
					var f = function() {
						response.write("data: " + Date.now() + "\n\n");
						timeoutId = setTimeout(f, 1000);
					};
				
					f();
				
					response.on("close", function () {
						clearTimeout(timeoutId);
					});
				}).listen(port);
			}
		});
	}
});