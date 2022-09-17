// const CH_SQL = require("./ddl-clickhouse.js");
// const PG_SQL = require("./ddl-clickhouse.js");

const MAP = {
    "application/json": (data, res)=> res.json(data),
    // "application/sql+clickhouse": CH_SQL,
    // "application/sql+postgresql": PG_SQL,
    // "application/sql": CH_SQL,
}

function responseFormatter(req, res) {
    for(let acc in MAP) {
        if (req.accepts(acc))
            return MAP[acc](req.resultData || {}, res)
    }
    res.json(req.resultData || {});
}

module.exports = {responseFormatter}
