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

function ATSDSocketClient(port, atsdUrl, logSystem) {
    if(!logSystem){
        logSystem = console;
    }
    if(!port){
        return {error: 'Missind required paramter: ATSD PORT'};
    }
    if(!atsdUrl){
        return {error: 'Missind required paramter: ATSD HOST'};
    }

    this._atsdUrl = atsdUrl;
    this._port = port;
    this.count = 0;
    this.client = new Socket();
    this.client.on('data', function (data){
        logSystem.log(data.toString());
    });

    this.client.on('connect', function () {
        //this.client.setKeepAlive(true,1000);
        logSystem.log('server: STARTUP -- AxiBase: Connected to ' + atsdUrl + ' on port ' + port);
    });

    this.client.on('error', function(err) {
        logSystem.error('Axibase: Socket error:', err.message);
    });

    this.client.on('close', function(err) {
        logSystem.log('Axibase: Socket close:', err.message);
        logSystem.log('Trying to reconnect');
        this.connect();
    });

    this.client.on('timeout', function(err) {
        logSystem.error('Axibase: Socket error:', err.message);
        logSystem.log('Trying to reconnect');
        this.connect();
    });
}

ATSDSocketClient.prototype.connect = function (cb) {
    this.client.connect(this._port, this._atsdUrl, function () {
        if(cb) {
            cb();
        }
    });
};

ATSDSocketClient.prototype.write = function (metric) {
    this.client.write(metric);
    this.count++;
};

ATSDSocketClient.prototype.end = function () {
    this.client.end();
};


