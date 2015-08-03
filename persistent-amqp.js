'use strict';

const EventEmitter = require('events').EventEmitter;
const amqp = require('amqplib');

const channelMethods = [
    'close', 'assertQueue', 'checkQueue', 'deleteQueue', 'purgeQueue', 'bindQueue', 'unbindQueue', 'assertExchange',
    'checkExchange', 'deleteExchange', 'bindExchange', 'unbindExchange', 'publish', 'sendToQueue', 'consume', 'cancel',
    'get', 'ack', 'ackAll', 'nack', 'nackAll', 'reject', 'prefetch', 'recover'
];

class PersistentAMQP extends EventEmitter {
    constructor(connectString, options) {
        super();
        this._options = {
            confirmChannel: options.confirmChannel || false
        };
        this._isConnected = false;
        this._isOpen = false;
        this._isTerminating = false;
        this.connectString = connectString;
        this._connect();
    }

    terminate() {
        this._isTerminating = true;
        this._clearNextConnectTimer();
        this._clearNextCreateChannelTimer();
        if (this._isConnected) {
            this.connection.close();
        }
    }

    isConnected() {
        return this._isConnected;
    }

    isOpen() {
        return this._isOpen;
    }

    isTerminating() {
        return this._isTerminating;
    }

    _connect() {
        const self = this;
        this._clearNextConnectTimer();
        if (this._isConnected) {
            this._createChannel();
            return;
        }
        amqp.connect(this.connectString).then(conn => {
            self.connection = conn;
            self._isConnected = true;
            console.log('AMQP Connection Opened');
            conn.on('close', self._onConnectionClose.bind(self));
            conn.on('error', self._onConnectionError.bind(self));
            self._createChannel();
            self.emit('connect');
        }).catch(err => {
            console.error('AMQP Connection', err.toString()); // TODO: replace error printout
            self._scheduleNextConnect();
        });
    }

    _onConnectionClose() {
        this._isConnected = false;
        this._isOpen = false;
        console.error('AMQP Connection Closed');
        this.emit('disconnect');
        this._scheduleNextConnect();
    }

    _onConnectionError(err) {
        console.error('AMQP Connection', err.toString());
    }

    _createChannel() {
        const self = this;

        this._clearNextCreateChannelTimer();
        if (this._isOpen) {
            return;
        }

        this.connection[this._options.confirmChannel ? 'createConfirmChannel' : 'createChannel']().then(chan => {
            self.channel = chan;
            self._isOpen = true;
            console.log('AMQP Channel Opened');
            chan.on('close', self._onChannelClose.bind(self));
            chan.on('error', self._onChannelError.bind(self));
            self._exposeChannelMethods();
            self.emit('open');
        }).catch(err => {
            console.error('AMQP Channel', err.toString()); // TODO: replace error printout
            self._scheduleNextCreateChannel();
        });
    }

    _onChannelClose() {
        console.error('AMQP Channel Closed');
        this._isOpen = false;
        this.emit('close');
        if (this._isConnected && !this._isTerminating) {
            this._scheduleNextCreateChannel();
        }
    }

    _onChannelError(err) {
        console.error('AMQP Channel', err.toString());
    }

    _scheduleNextConnect() {
        const self = this;
        if (this._isTerminating) {
            return;
        }
        this._clearNextCreateChannelTimer();
        this.nextConnectTimer = setTimeout(() => {
            self.nextConnectTimer = undefined;
            self._connect();
        }, 1000);
    }

    _clearNextConnectTimer() {
        if (this.nextConnectTimer) {
            clearTimeout(this.nextConnectTimer);
            this.nextConnectTimer = undefined;
        }
    }

    _scheduleNextCreateChannel() {
        const self = this;
        if (this._isTerminating) {
            return;
        }
        this.nextCreateChannelTimer = setTimeout(() => {
            self.nextCreateChannelTimer = undefined;
            self._createChannel();
        });
    }

    _clearNextCreateChannelTimer() {
        if (this.nextCreateChannelTimer) {
            clearTimeout(this.nextCreateChannelTimer);
            this.nextCreateChannelTimer = undefined;
        }
    }

    _exposeChannelMethods() {
        const self = this;
        channelMethods.forEach(methodName => {
            self[methodName] = function () {
                return self.channel[methodName].apply(self.channel, arguments);
            };
        });
    }
}

let persistentConnections = {};

module.exports = function (connectString, options) {
    if (!persistentConnections[connectString]) {
        persistentConnections[connectString] = new PersistentAMQP(connectString, options || {});
    }

    return persistentConnections[connectString];
};
