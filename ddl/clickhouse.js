const CH_CONSUMER_GROUP = 'clickhouse';
const CH_CONSUMERS = 1;
const CH_CONSUMERS_BLOCK_SIZE = 10;
const CH_CONSUMER_TREADS = 0;

const {ClickHouse}  = require("clickhouse");
const fs = require("fs-extra");
const {Router} = require("express");
const path = require("path");
const {responseFormatter} = require("../formats");
const _ = require("lodash");
const Ajv = require("ajv");


const CDA_MAP = {
    "BL": "Boolean",
    "BN": "Boolean",
    "ED": "Blob",
    "INT": "Int32",
    "REAL": "Float",
    "PQ": "Tuple(Float, String)",
    "MO": "Tuple(Float, String)",
    "TS": "Datetime",
    "SET": "Array(String)",
    "LIST": "Array(String)",
    "BAG": "Array(String)",
    "IVL": "Tuple(String, String)",
    "HIST": "Array(Map(String, String))",
    "PIVL": "Tuple(Int32, Int32)"
}
const DEFAULT_LEAF_NODES_TYPE = "String"
class CLickHouseDDL {
    constructor({urlPrefix, host, port, database, schemaTable, username, password, kafkaHosts, topicTemplate } ) {
        this.urlPrefix = urlPrefix;
        this.kafkaHosts = kafkaHosts || "localhost:9092";
        this.kafkaTopicTemplate = topicTemplate || "{{templateId}}";
        this.database = database;
        this.clickhouse = new ClickHouse({
            url: host,
            port: port || 8123,
            debug: false,
            basicAuth: (username ? {username, password: password || ''} : null),
            isUseGzip: false,
            trimQuery: false,
            usePost: false,
            format: "json", // "json" || "csv" || "tsv"
            raw: false,
            config: {
                database,
                // session_id                              : 'session_id if neeed',
                session_timeout: 60,
                output_format_json_quote_64bit_integers: 0,
                enable_http_compression: 0
            },
            // This object merge with request params (see request lib docs)
            // reqParams: {
            //     ...
            // }
        });
        this.schemaTable = schemaTable;
    }

    router () {
        const that = this;
        const router = new Router("/");
        router.get("/:id/:version?", async (req, res, next) => {
            const {version, id}  = req.params;
            if (version && !version.match(/^\d+$/)) {
                return next({status: 400, error: "Bad version"});
            }
            if (!id.match(/^[a-f\d\-.]+$/)) {
                return next({status: 400, error: "Bad schema id"});
            }
            try {
                const q = `SELECT schema FROM ${that.schemaTable} WHERE id="${id}" ` + (version ? ` AND version=${version}` : "") +
                          ` ORDER BY created DESC LIMIT 1`;
                const qR = await that.clickhouse.query(q).toPromise();
                if (!qR && !qR.length) return next({status: 404});
                const {schema}  = qR[0];
                req.resultData = _.isString(schema) ? JSON.parse(schema) : schema;
                return next();
            } catch(e) {
                return next({status:500, error:e});
            }
        }, responseFormatter)

        router.post("/:id/:version?", async (req, res, next) => {
            const {version, id}  = req.params;
            if (version && !version.match(/^\d+$/)) {
                return next({status: 400, error: "Bad version"});
            }
            if (!id.match(/^[a-f\d\-.]+$/)) {
                return next({status: 400, error: "Bad schema id"});
            }
            if (!req.body || _.isObject(req.body) || _.isEmpty(req.body)) {
                return next({status: 400, error: new Error("Bad request payload")})
            }

            const ajv = new Ajv();
            if (!ajv.validateSchema(req.body)) {
                return next({status: 400, error: ajv.errors})
            }
            try {
                let newVersion;
                if (version) {
                    const qR = await that.clickhouse.query(`SELECT created FROM '${that.schemaTable}' WHERE id="${id}" AND version=${version} LIMIT 1`).toPromise();
                    if (qR && qR.length) return next({status: 409, error:`Schema already exists since ${qR[0].created}`});
                    newVersion = version;
                } else {
                    const qR = await that.clickhouse.query(`SELECT MAX(version) as v FROM '${that.schemaTable}' WHERE id="${p}"`).toPromise();
                    newVersion = (qR && qR.length ? Number(qR[0].v) :0 ) + 1 ;
                }
                const schema = req.body;
                data["$id"] = `${that.urlPrefix}/${id}/${newVersion}`;
                await that.sync(id, newVersion, schema);
                await that.clickhouse.insert(`INSERT INTO ${that.schemaTable} (id, version, schema) `,
                    [{id, version:newVersion, schema:JSON.stringify(schema) }]).toPromise();
                req.resultData = schema;
                return next();
            } catch(e) {
                return next({status:500, error:e});
            }
        })
        return router;
    }

    static nodeTypeMapper(node, parentKey) {
        if (!node || !node.type || node.virtual) return;
        if (["string", "number", "boolean"].indexOf(node.type)>=0) {
            if (node.xml_type)  {
                return {name: parentKey, type:CDA_MAP[node.xml_type] || DEFAULT_LEAF_NODES_TYPE};
            } else {
                return {name: parentKey, type: DEFAULT_LEAF_NODES_TYPE};
            }
        }
        if (node.type==="array") {
            if (!node.items) throw new Error("'items' isn't defined: "+JSON.stringify(node))
            if (!node.items)  return DEFAULT_LEAF_NODES_TYPE;
            let itemTypes = CLickHouseDDL.nodeTypeMapper(node.items, parentKey);
            if (itemTypes && Array.isArray(itemTypes)) {
                itemTypes = _.uniq(r.map(a => a.type));
            } else if (itemTypes && itemTypes.type) {
                itemTypes = itemTypes.type
            } else itemTypes= DEFAULT_LEAF_NODES_TYPE
            if (!Array.isArray(itemTypes)) {
                 return {name: parentKey, isArray:true, type:`Array(${itemTypes})`, items:itemTypes };
            }
            if (itemTypes.length===1) {
                return {name: parentKey, isArray:true, type: `Array(${itemTypes[0]})`, items:itemTypes[0]};
            } else
                return {name: parentKey, isArray:true, type: `Array(${DEFAULT_LEAF_NODES_TYPE})`, items: DEFAULT_LEAF_NODES_TYPE };
        }
        if (node.type==="object") {
            if (!node.properties) throw new Error("'properties' isn't defined: "+JSON.stringify(node))
            const children = [];
            for(let p in node.properties) {
                const a = CLickHouseDDL.nodeTypeMapper(node.properties[p], p);
                if (a) children.push(a);
            }
            if (!parentKey) return children;
            const mapTYpe = _.uniq(children.map(a=>a.type));
            if (mapTYpe.length===1) {
                return {name: parentKey, type: `Map(String, ${mapTYpe[0]})`, items:mapTYpe[0]}
            }
            else {
                return {name: parentKey, type: `Map(String, ${DEFAULT_LEAF_NODES_TYPE})`, items: DEFAULT_LEAF_NODES_TYPE}
            }
        }
    }

    async sync(id, version, schema) {
        const fields = CLickHouseDDL.nodeTypeMapper(schema);
        if (!fields || !Array.isArray(fields)) throw new Error("Schema doesn't have any storable fields");
        const database = this.database;
        const destTopicName = this.kafkaTopicTemplate.replace("{{templateId}}", `${id}.${version}`);
        const queries = [
            `CREATE TABLE IF NOT EXISTS ${database}.kafka_${destTopicName} ("msg" String ) ENGINE = Kafka SETTINGS \
                 kafka_broker_list = '${this.kafkaHosts}', \
                 kafka_topic_list = '${destTopicName}', \
                 kafka_group_name = ${CH_CONSUMER_GROUP}, \
                 input_format_skip_unknown_fields=1, \
                 kafka_format = 'JSONAsString', \
                 kafka_thread_per_consumer = ${CH_CONSUMER_TREADS}, \
                 kafka_num_consumers = ${CH_CONSUMERS}, \
                 kafka_max_block_size = ${CH_CONSUMERS_BLOCK_SIZE};`,

            `CREATE TABLE IF NOT EXISTS ${database}.'stg_${destTopicName}' (`
                + fields.map(f=>` '${f.name}' Nullable(${f.type})`).join(", \n")
                +`, \n 'topic' String, 'offset'  UInt64, 'timestamp' DateTime) ENGINE = MergeTree ORDER BY ('offset')`,

            `CREATE MATERIALIZED VIEW IF NOT EXISTS ${database}.'kafka_mv_${destTopicName}' TO ${database}.'stg_${destTopicName}' AS select \
                CAST(JSONExtractKeysAndValues("msg",'String'),'Map(String, String)') as d, \
                _topic as 'topic', _offset as 'offset', _timestamp as 'timestamp', \n`+
                fields.map(f=>
                    (f.isArray ?
                        ` arrayMap(s -> CAST(JSONExtractKeysAndValues(s,'String'),'${f.type}'), JSONExtractArrayRaw(d['${f.name}'])) as "${f.name}" `:
                        ` CAST(d['${f.name}'], Nullable('${f.type}')) as "${f.name}" `)
                ).join(", \n")+
                `\n FROM ${database}.'kafka_${destTopicName}';`
        ];
        for(const query of queries) {
            await this.clickhouse.query(query).toPromise();
        }
    }

    async warmUp() {
        const query = `CREATE TABLE ${database}.'${this.schemaTable}' IF NOT EXISTS (
                id String,
                version UInt32,
                created DateTime,
                schema JSON
            ) ENGINE=MergeTree(created, (id, version), 8192)`;
        await this.clickhouse.query(query).toPromise();
        for await (const row of this.clickhouse.query(`SELECT id, version, schema FROM '${this.schemaTable}' `).stream()) {
            // console.log(row);
            try {
                await this.sync(row.id, row.version, row.schema);
            } catch (e) {
                console.error("Re-creating schema error "+e.message);
            }
        }
    }
}

module.exports = {CLickHouseDDL};


