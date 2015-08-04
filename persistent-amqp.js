'use strict';

const EventEmitter = require('events').EventEmitter;
const amqp = require('amqplib');

const channelMethods = [
    'assertQueue', 'checkQueue', 'deleteQueue', 'purgeQueue', 'bindQueue', 'unbindQueue', 'assertExchange',
    'checkExchange', 'deleteExchange', 'bindExchange', 'unbindExchange', 'publish', 'sendToQueue', 'consume', 'cancel',
    'get', 'ack', 'ackAll', 'nack', 'nackAll', 'reject', 'prefetch', 'recover'
];

let connections = {};

class AMQPChannelWrapper extends EventEmitter {

    constructor(connectionWrapper, isConfirmChannel, channelReopenInterval) {
        super();
        this._isConfirmChannel = isConfirmChannel;
        this._openHooks = [];
        this._closeHooks = [];
        this._exposeChannelMethods();
        this._connectionWrapper = connectionWrapper;
        this._channelReopenInterval = channelReopenInterval;
        connectionWrapper.on('connect', this._createChannel.bind(this));
        connectionWrapper.on('terminate', this._onConnectionTerminate.bind(this));
    }

    close() {
        const self = this;
        if (!this._channel || this._terminating) {
            return;
        }
        this._terminating = true;
        this._executeHooks(this._closeHooks).then(() => self._channel.close()); // TODO: catch hook errors
        this._channel.close();
    }

    get open() {
        return this._channel !== undefined;
    }

    addOpenHook(hookFunction) {
        this._openHooks.push(hookFunction);
    }

    addCloseHook(hookFunction) {
        this._closeHooks.push(hookFunction);
    }

    _createChannel() {
        const self = this;
        if (this._channel || !this._connectionWrapper.connected) {
            return;
        }

        let createMethodName = this._isConfirmChannel ? 'createConfirmChannel' : 'createChannel';
        this._connectionWrapper._connection[createMethodName]().then((chan) => {
            self._channel = chan;
            console.log('AMQP Channel Opened');
            chan.on('close', self._onChannelClose.bind(self));
            chan.on('error', self._onChannelError.bind(self));
            self._executeHooks(self._openHooks).then((res) => self.emit('open', res)); // TODO: catch hook errors
        }).catch(err => {
            console.error('AMQP Channel', err.toString());
            self._scheduleNextCreateChannel();
        });
    }

    _executeHooks(hooks) {
        if (hooks.length === 0) {
            return Promise.resolve();
        }
        let promises = [];
        hooks.forEach(hookFn => {
            let res = hookFn();
            if (Array.isArray(res)) {
                promises = promises.concat(res);
            }
            else {
                promises.push(res);
            }
        });

        return Promise.all(promises);
    }

    _onChannelClose() {
        console.error('AMQP Channel Closed');
        this._channel = undefined;
        this.emit('close');
        if (this._connectionWrapper.connected && !this._terminating) {
            this._scheduleNextCreateChannel();
        }
        else if (this._connectionWrapper.connected) {
            this._connectionWrapper._channelTerminated();
        }
    }

    _onChannelError(err) {
        console.error('AMQP Channel', err.toString());
    }

    _scheduleNextCreateChannel() {
        const self = this;
        if (this.terminating) {
            return;
        }
        this._clearNextCreateChannelTimer();
        this.nextCreateChannelTimer = setTimeout(() => {
            self.nextCreateChannelTimer = undefined;
            self._createChannel();
        }, this._channelReopenInterval);
    }

    _clearNextCreateChannelTimer() {
        if (this.nextCreateChannelTimer) {
            clearTimeout(this.nextCreateChannelTimer);
            this.nextCreateChannelTimer = undefined;
        }
    }

    _onConnectionTerminate() {
        this.close();
    }

    _exposeChannelMethods() {
        const self = this;
        channelMethods.forEach(methodName => {
            self[methodName] = function () {
                if (!self._channel) {
                    throw new Error('Method call on a closed AMQP channel'); // TODO: queue method calls or throw error.
                }
                return self._channel[methodName].apply(self._channel, arguments);
            };
        });
    }

}

class AMQPConnectionWrapper extends EventEmitter {

    constructor(options) {
        super();
        this._options = {
            host: options.host,
            confirmChannel: options.confirmChannel || false,
            reconnectInterval: options.reconnectInterval || 1000,
            channelReopenInterval: options.channelReopenInterval || 100
        };
        this._channels = [];
        this._numChannels = 0;
        this._connect();
    }

    createChannel(isConfirmChannel) {
        let channel = new AMQPChannelWrapper(this, isConfirmChannel, this._options.channelReopenInterval);
        this._numChannels++;
        if (this._connection) {
            channel._createChannel();
        }

        return channel;
    }

    createConfirmChannel() {
        return this.createChannel(true);
    }

    close() {
        this._clearNextConnectTimer();
        if (this._terminating && this._numChannels > 0 || !this._connection) {
            return;
        }
        this._terminating = true;
        this.emit('terminate');
        if (this._numChannels === 0) {
            this._connection.close();
        }
    }

    _connect() {
        const self = this;
        this._clearNextConnectTimer();
        if (this._connection) {
            return;
        }
        amqp.connect(this._options.host).then(conn => {
            self._connection = conn;
            console.log('AMQP Connection Opened');
            conn.on('close', self._onConnectionClose.bind(self));
            conn.on('error', self._onConnectionError.bind(self));
            self.emit('connect');
        }).catch(err => {
            console.error('AMQP Connection', err.toString());
            self._scheduleNextConnect();
        });
    }

    _onConnectionClose() {
        this._connection = undefined;
        console.error('AMQP Connection Closed');
        this.emit('disconnect');
        this._scheduleNextConnect();
    }

    _onConnectionError(err) {
        console.error('AMQP Connection', err.toString());
    }

    _scheduleNextConnect() {
        const self = this;
        if (this._terminating) {
            return;
        }
        this.nextConnectTimer = setTimeout(() => {
            self.nextConnectTimer = undefined;
            self._connect();
        }, this._options.reconnectInterval);
    }

    _clearNextConnectTimer() {
        if (this.nextConnectTimer) {
            clearTimeout(this.nextConnectTimer);
            this.nextConnectTimer = undefined;
        }
    }

    _channelTerminated() {
        this._numChannels--;
        if (this._connection && this._numChannels === 0) {
            console.log('AMQP Connection: all channels closed, terminating');
            this.close();
        }
    }

    get connected() {
        return this._connection !== undefined;
    }

    get terminating() {
        return this._terminating;
    }
}

module.exports = {
    connection(options) {
        let opts = typeof options == 'object' ? options : {};
        let host = opts.host || 'amqp://localhost';
        opts.host = host;
        if (!connections[host]) {
            connections[host] = new AMQPConnectionWrapper(opts);
        }

        return connections[host];
    },

    closeAllConnections() {
        Object.keys(connections).forEach(connection => {
            connection.close();
        });
    }
};
