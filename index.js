const IP  = process.env.NODE_IP_WHITELIST || "0.0.0.0";
const params = require("./config");
const API_VERSION = "v1";
const http = require("http");
const express = require("express");
const compression = require('compression');
const bodyParser = require('body-parser');
const app = express();
const server = http.createServer(app);
const {CLickHouseDDL} = require("./ddl/clickhouse.js")
const example = require("./example.json");
console.log(JSON.stringify(CLickHouseDDL.nodeTypeMapper(example), null, 2))

// app.use(compression());
// app.use(bodyParser.json({ limit: '50mb' }));

// const clickhouseDDL = new ClickhouseDDL({urlPrefix: params.domain+"/"+API_VERSION, ...params})
// clickhouseDDL.warmUp();
// app.use("/"+API_VERSION, clickhouseDDL.router() );
//
// server.listen(params.httpPort, IP, function () {
//     console.log(`Express server listening on ${params.httpPort}. Listen to ${IP}`);
// });
