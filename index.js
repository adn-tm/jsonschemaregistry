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
// const example = require("./example.json");
 console.log(JSON.stringify(params, null, 2));

app.use(compression());
app.use(bodyParser.json({ limit: '50mb' }));

const clickhouseDDL = new CLickHouseDDL({urlPrefix: params.domain+"/"+API_VERSION, ...params})
async function waitForWarm() {
    let lastE;
    for(let i=0; i<20; i++) {
        try {
            await clickhouseDDL.warmUp()
            return;
        } catch (e) {
            lastE = e;
        }
    }
    throw lastE;
}

waitForWarm().catch(e => {
    console.error("Database warm up error", e);
    process.exit(-1);
})

app.use("/"+API_VERSION, clickhouseDDL.router() );

app.use((err, req, res, next)=>{
    if (res.headersSent) {
        return next(err)
    }
    console.log(err);
    res.status(err.status || 500).json(err);
})

server.listen(params.httpPort, IP, function () {
    console.log(`Express server listening on ${params.httpPort}. Listen to ${IP}`);
});
