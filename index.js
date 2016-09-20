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

function ATSDSocketClient(port, atsdUrl) {
    this._atsdUrl = atsdUrl;
    this._port = port;
    this.client = new Socket();
    this.client.on('data', function (data){
        console.log(data.toString());
    });

    this.client.on('connect', function () {
        //this.client.setKeepAlive(true,1000);
        console.log('server: STARTUP -- AxiBase: Connected to ' + atsdUrl + ' on port ' + port);
    });

    this.client.on('error', function(err) {
        console.log('Axibase: Socket error:', err.message);
    });

    this.client.on('close', function(err) {
        console.log('Axibase: Socket close:', err.message);
    });

    this.client.on('timeout', function(err) {
        console.log('Axibase: Socket error:', err.message);
    });
}





ATSDSocketClient.prototype.connect = function (cb) {
    this.client.connect(this._port, this._atsdUrl, function () {
        console.log('Connected To AxiBase');
        if(cb) {
            cb();
        }
    });
};

ATSDSocketClient.prototype.write = function (message) {

    var that = this;
    var tags = message.tags;
    var metrics = message.metrics;
    var messageBase = 'series e:agent ';
    if(metrics){
        metrics.forEach(function (metric) {
            var metricName = metric.metric_name;
            var metricMessageBase = messageBase + parseTags(_.defaults(metric.tags, tags));
            metric.timeseries.forEach(function(single){
                var messageToWrite = metricMessageBase + parseMetricValues(metricName, single) + '\n';
                that.insertData(messageToWrite);
            });
        });
    }
};



ATSDSocketClient.prototype.insertData = function (metric) {
    this.client.write(metric);
};

ATSDSocketClient.prototype.end = function () {
    this.client.end();
};

function parseTags(tags) {
    var keys = Object.keys(tags), tempString = '';
    keys.forEach(function (key) {
        tempString = tempString.concat('t:' + key + '=' + tags[key] + ' ');
    });
    return tempString;
}

function parseMetricValues(metricName, value) {
    if (value[0]) {
        return 's:' + value[0] + ' ' + 'm:' + metricName.trim() + '=' + value[1] + ' ';
    }
}


