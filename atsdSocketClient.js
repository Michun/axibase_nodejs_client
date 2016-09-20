var Socket = require('net').Socket;
var _ = require('lodash');

exports.ATSDSocketClient = ATSDSocketClient;

/**
 * ATSD socket client consturctor
 *
 * @class
 *
 * @constructor
 *
 * @param {Number} atsdUrl
 * @param {String} port
 */

var client = undefined;
function ATSDSocketClient(port, atsdUrl) {
    this._atsdUrl = atsdUrl;
    this._port = port;
    client = new Socket();
    client.on('data', function (data){
        console.log(data.toString());
    });
}



ATSDSocketClient.prototype.connect = function (cb) {
    client.connect(this._port, this._atsdUrl, function () {
        console.log('Connected To AxiBase');
        cb();
    });
};

ATSDSocketClient.prototype.write = function (message) {

    var that = this;
    if(message.mime_type === 'metric/timeseries' && message.metrics){
        var insertTemp = [];
        var metricsGrouped = _.groupBy(message.metrics, function(met){return met.timeseries[0][0]});
        var timestamps = Object.keys(metricsGrouped);
        timestamps.forEach(function (timestamp) {
            that.parseMessage(timestamps[timestamp]).forEach(function (singleMetric) {
                insertTemp.push(singleMetric.trim());
            });
        });
        that.insertData(insertTemp.join("\n"), client);
    }


     /*   if (typeof message.metrics === 'object' && message.length) {
            var insertTemp = [];
            message.metrics.forEach(function (metric) {
                that.parseMessage(metric).forEach(function (singleMetric) {
                    insertTemp.push(singleMetric.trim());
                });
            });
            that.insertData(insertTemp.join("\n"), client);
        } else if (typeof message.metrics === 'object') {
            var insertTemp = [];
            that.parseMessage(message.metrics).forEach(function (singleMetric) {
                insertTemp.push(singleMetric.trim());
            });
            that.insertData(insertTemp.join("\n"), client);
        } else if (message.indexOf('series') > -1) {
            that.insertData(message, client);
        } else {
            console.error('Not supported type');
            return false;
        }
*/
};



ATSDSocketClient.prototype.insertData = function (metric, client) {
    client.write('debug '+ metric, function(){
        console.log('metric:','debug '+ metric, 'is written');
    });
    //client.end();
};

ATSDSocketClient.prototype.end = function () {
    this.client.end();
};

ATSDSocketClient.prototype.parseMessage = function (message) {
    var metricArray = [], parsedString = 'series ';

    if (message.entity) {
        parsedString = parsedString.concat('e:' + message.entity + ' ');
    } else {
        parsedString = parsedString.concat('e:agent ');
    }

    if (message.tags) {
        parsedString = parsedString.concat(parseTags(message.tags));
    }
    if (message.data) {
        parseMetric(message.metric, message.data, parsedString, metricArray);
    }
    return metricArray;
};

function parseTags(tags) {
    var keys = Object.keys(tags), tempString = '';
    keys.forEach(function (key) {
        tempString = tempString.concat('t:' + key + '=' + tags[key] + ' ');
    });
    return tempString;
}

function parseMetric(metricName, data, parsedString, metricArray) {
    if (data.length) {
        data.forEach(function (value) {
            metricArray.push(parsedString + parseMetricValues(metricName, value));
        });
    } else if (typeof data === 'object') {
        metricArray.push(parsedString + parseMetricValues(metricName, data));
    }
}

function parseMetricValues(metricName, value) {
    if (value.d) {
        return 's:' + toTimestamp(value.d) + ' ' + 'm:' + metricName.trim() + '=' + value.v + ' ';
    } else if (value.s) {
        return 's:' + value.s + ' ' + 'm:' + metricName + '=' + value.v + ' ';
    } else if (value.ms) {
        return 'ms:' + value.ms + ' ' + 'm:' + metricName + '=' + value.v + ' ';
    }
}

function toTimestamp(strDate) {
    var datum = Date.parse(strDate);
    return datum/1000;
}

/*

 */




