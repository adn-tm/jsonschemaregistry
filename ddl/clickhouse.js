const NO_SQL = false; // output DDL SQL, but no apply

const CH_CONSUMER_GROUP = "clickhouse";
const CH_CONSUMERS = 1;
const CH_CONSUMERS_BLOCK_SIZE = 10;
const CH_CONSUMER_TREADS = 0;

const {ClickHouse}  = require("clickhouse");
const {Router} = require("express");
const {responseFormatter} = require("../formats");
const _ = require("lodash");
const Ajv = require("ajv");
const CDA_MAP = require("../config/cda-map.json")
const DEFAULT_LEAF_NODES_TYPE = "String"

class CLickHouseDDL {
    constructor({urlPrefix, host, port, database, schemaTable, username, password, kafkaHosts, topicTemplate } ) {
        this.urlPrefix = urlPrefix;
        this.kafkaHosts = kafkaHosts || "localhost:9092";
        this.kafkaTopicTemplate = topicTemplate || "{{templateId}}";
        this.database = database;
        const opts = {
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
                // session_id                              : "session_id if neeed",
                session_timeout: 60,
                output_format_json_quote_64bit_integers: 0,
                enable_http_compression: 0
            },
            // This object merge with request params (see request lib docs)
            // reqParams: {
            //     ...
            // }
        }
        console.log(opts)
        this.clickhouse = new ClickHouse(opts);
        this.schemaTable = schemaTable;
    }

    router() {
        const that = this;
        const router = new Router("/");
        const database = this.database;
        router.get("/:id", async (req, res, next) => {
            const {id}  = req.params;
            if (!id.match(/^[a-f\d\-.]+$/)) {
                return next({status: 400, error: "Bad schema id"});
            }
            try {
                const q = `SELECT id, version, jsonschema, created FROM ${database}."${that.schemaTable}" WHERE id='${id}' ORDER BY version DESC`;
                console.log("---SQL: \n"+q);
                if (NO_SQL) {
                    return next({status: 200, id, debug: true});
                }
                const qR = await that.clickhouse.query(q).toPromise();
                if (!qR || !qR.length) return next({status: 404});
                const {jsonschema}  = qR[0];
                req.resultData = qR;
                return next();
            } catch(e) {
                return next({status:500, error:e});
            }
        }, responseFormatter)

        router.get("/:id/:version", async (req, res, next) => {
            const {version, id}  = req.params;
            if (NO_SQL) return next({status: 200, version, id, debug:true });
            if (version!=="@" && !version.match(/^\d+$/)) {
                return next({status: 400, error: "Bad version"});
            }
            if (!id.match(/^[a-f\d\-.]+$/)) {
                return next({status: 400, error: "Bad schema id"});
            }
            try {
                const q = `SELECT jsonschema FROM ${database}."${that.schemaTable}" WHERE id='${id}' ` + (version!="@" ? ` AND version=${version}` : "") +
                    ` ORDER BY created DESC LIMIT 1`;
                const qR = await that.clickhouse.query(q).toPromise();
                if (!qR && !qR.length) return next({status: 404});
                const {jsonschema}  = qR[0];
                req.resultData = _.isString(jsonschema) ? JSON.parse(jsonschema) : jsonschema;
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
            if (!req.body || !_.isObject(req.body) || _.isEmpty(req.body)) {
                console.log("BODY:", req.body)
                return next({status: 400, error: new Error("Bad request payload")})
            }

            const ajv = new Ajv();
            if (!ajv.validateSchema(req.body)) {
                return next({status: 400, error: ajv.errors})
            }
            try {
                let newVersion;
                if (NO_SQL) {
                    newVersion = version || 1;
                } else {
                    if (version) {
                        const q = `SELECT created FROM ${database}."${that.schemaTable}" WHERE id='${id}' AND version=${version} LIMIT 1`
                        console.log("---SQL: \n"+q)
                        const qR = await that.clickhouse.query(q).toPromise();
                        if (qR && qR.length) return next({
                            status: 409,
                            error: `Schema already exists since ${qR[0].created}`
                        });
                        newVersion = version;

                    } else {
                        const qR = await that.clickhouse.query(`SELECT MAX(version) as v FROM ${database}."${that.schemaTable}" WHERE id='${p}'`).toPromise();
                        newVersion = (qR && qR.length ? Number(qR[0].v) : 0) + 1;
                    }
                }
                const schema = req.body;
                schema["$id"] = `${that.urlPrefix}/${id}/${newVersion}`;
                await that.sync(id, newVersion, schema);
                const q = `INSERT INTO ${database}."${that.schemaTable}" (id, version, jsonschema) `
                console.log("---SQL: \n"+q, [{id, version:newVersion, jsonschema:JSON.stringify(schema) }])
                if (!NO_SQL) {
                    await that.clickhouse.insert(q, [{
                        id,
                        version: newVersion,
                        jsonschema: JSON.stringify(schema)
                    }]).toPromise();
                }
                req.resultData = schema;
                return next();
            } catch(e) {
                return next({status:500, error:e});
            }
        }, responseFormatter)
        return router;
    }

    static nodeTypeMapper(node, parentKey) {
        if (!node || !node.type || node.virtual) return;
        //   "TS": "DateTime",
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
            const tupleDef=children.map(f=>`"${f.name}" ${f.type} `).join(", ");
            return {name: parentKey, isMap:true,  type: `Tuple(${tupleDef})`}
            // const mapTYpe = _.uniq(children.map(a=>a.type));
            // if (mapTYpe.length===1) {
            //     return {name: parentKey, isMap:true, type: `Map(String, ${mapTYpe[0]})`, items:mapTYpe[0]}
            // }
            // else {
            //     return {name: parentKey, isMap:true,  type: `Map(String, ${DEFAULT_LEAF_NODES_TYPE})`, items: DEFAULT_LEAF_NODES_TYPE}
            // }
        }
    }
    static schemaIdFromOID(oid) {
        const p = (oid || "").split(".");
        if (p.length<3) return;
        return {id:p[p.length-3], version:[p.length-1]}
    }
    async sync(id, version, schema) {
        if (typeof schema === "string") schema = JSON.parse(schema);
        const fields = CLickHouseDDL.nodeTypeMapper(schema);
        if (!fields || !Array.isArray(fields)) throw new Error("Schema doesn't have any storable fields");
        const database = this.database;
        const tablesSuffix = `${id}_${version}`;
        const destTopicName = this.kafkaTopicTemplate
            .replace("{{id}}", id)
            .replace("{{version}}", version)
            .replace("{{templateId}}", `${id}_${version}`);
        let withSentence="";
        const withFields=fields.filter(f=>(f.isMap || f.isArray)).map(f=>(
            f.isArray ?
                ` arrayMap(s -> JSONExtract(s, '${f.items}'), JSONExtractArrayRaw(d['${f.name}'])) as "${f.name}" `:
                ` CAST(d['${f.name}'], '${f.type}') as "${f.name}" `));
        if (withFields.length) withSentence="WITH "+withFields.join(", \n")
        const queries = [
            `CREATE TABLE IF NOT EXISTS ${database}."kafka_${tablesSuffix}" ("msg" String ) ENGINE = Kafka SETTINGS \
                 kafka_broker_list = '${this.kafkaHosts}', \
                 kafka_topic_list = '${destTopicName}', \
                 kafka_group_name = '${CH_CONSUMER_GROUP}', \
                 input_format_skip_unknown_fields=1, \
                 kafka_format = 'JSONAsString', \
                 kafka_thread_per_consumer = ${CH_CONSUMER_TREADS}, \
                 kafka_num_consumers = ${CH_CONSUMERS}, \
                 kafka_max_block_size = ${CH_CONSUMERS_BLOCK_SIZE};`,

            `CREATE TABLE IF NOT EXISTS ${database}."stg_${tablesSuffix}" (`
            + fields
                .map(f=>(f.isArray || f.isMap)? ` "${f.name}" ${f.type}` :` "${f.name}" Nullable(${f.type})`)
                .join(", \n")
            +`, \n "topic" String, "offset"  UInt64, "timestamp" DateTime) ENGINE = MergeTree ORDER BY ("offset")`,

            `CREATE MATERIALIZED VIEW IF NOT EXISTS ${database}."kafka_mv_${tablesSuffix}" TO ${database}."stg_${tablesSuffix}" AS `+
            withSentence
            +` select \
                CAST(JSONExtractKeysAndValues("msg",'String'),'Map(String, String)') as d, \
                _topic as "topic", _offset as "offset", _timestamp as "timestamp", \n`+
            fields.map(f=> ((!f.isArray && !f.isMap ? ` CAST(d['${f.name}'], 'Nullable(${f.type})') as `:"")+`"${f.name}" `)
                // (f.isArray ?
                //     ` arrayMap(s -> JSONExtract(s, '${f.items}'), JSONExtractArrayRaw(d['${f.name}'])) as "${f.name}" `:
                //         (f.isMap ? ` CAST(d['${f.name}'], '${f.type}') as "${f.name}" `
                //             : ` CAST(d['${f.name}'], 'Nullable(${f.type})') as "${f.name}" `)
                // )
// arrayMap(s -> JSONExtract(s,'Tuple("typeCode" String , "text" String , "code" String , "pregnancyTerm" Int32 , "pregnancyTermUnit" String )'),
//     JSONExtractArrayRaw(d['REFDGN'])) as "REFDGN" ,
            ).join(", \n")+
            `\n FROM ${database}."kafka_${tablesSuffix}";`
        ];
        for(const query of queries) {
            console.log("---SQL: \n"+query)
            if (!NO_SQL) {
                // try {
                await this.clickhouse.query(query).toPromise();
                // } catch(e) {
                //
                // }
            }
        }
    }

    async warmUp() {
        const database = this.database;
        const query = `CREATE TABLE  IF NOT EXISTS ${database}."${this.schemaTable}" (
                id String,
                version UInt32,
                created DateTime,
                jsonschema String
            ) ENGINE= MergeTree ORDER BY ("id", "version")`;
        console.log("---SQL: \n"+query)
        if (!NO_SQL) {
            await this.clickhouse.query(query).toPromise();
            for await (const row of this.clickhouse.query(`SELECT id, version, jsonschema FROM ${database}."${this.schemaTable}" `).stream()) {
                // console.log(row);
                try {
                    await this.sync(row.id, row.version, row.jsonschema);
                } catch (e) {
                    console.error("Re-creating schema error " + e.message);
                }
            }
        }
    }
}

module.exports = {CLickHouseDDL};
