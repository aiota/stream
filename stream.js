var PORT = 30030;

var http = require("http");
var fs = require("fs");
var url = require("url");

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
}).listen(PORT);