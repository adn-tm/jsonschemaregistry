const argsProcessor = require('command-line-parser');
const options = require("./params.json");

function readParams() {
    const cmd = argsProcessor() || {};
    if (cmd.h || cmd.help) {
        console.log("XML 2 JSON stream convertor. Use with parameters:");
        if (cmd.env) {
            for (let p in options) {
                if (options[p].env)
                    console.log(`${options[p].env}=`+(options[p].default ? options[p].default : ""));
            }
            process.exit(0);
        }
        for (let p in options) {
            console.log(options[p].description);
            if (options[p].env)
                console.log(`\t\tIt can be passed by $${options[p].env} environment variable`);
            if (options[p].default)
                console.log(`\t\tDefault value is ${options[p].default}`);
            console.log(``);
        }
        process.exit(0);
    }


    function _opt(cmd, option) {
        return cmd[option.name] || cmd[option.short] || (option.env ? process.env[option.env] : undefined) || option.default;
    }

    const params = {};
    for (let p in options) {
        params[p] = _opt(cmd, options[p])
        if (!params[p] && options[p].required) {
            console.log(`Undefined required parameter: ${p}. Run with -h for help`);
            process.exit(1);
        }
    }
    return params;
}

module.exports = readParams();
