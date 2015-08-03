'use strict';

const amqpConnection = require('..').connection({
    host: 'amqp://localhost'
});

const createChannelsNum = 2;
const simulateChannelError = true;

function createChannel(idx) {

    let channel = amqpConnection.createChannel();
    let queueName;
    let booBooTimer;

    channel.addOpenHook(() => {
        return [channel.assertQueue(null, { durable: false })]; // assert random named queue on channel create
    });

    channel.addCloseHook(() => {
        return [channel.deleteQueue(queueName)]; // delete queue
    });

    channel.on('open', (openHookResults) => {
        queueName = openHookResults[0].queue; // store the name of the created queue
        console.log(`>> Channel #${idx} opened with queue named "${queueName}"`);

        // create badly behaving channel user
        if (idx === 0 && simulateChannelError) {
            booBooTimer = setTimeout(() => {
                channel.purgeQueue('not-existing-queue').catch((err) => {
                    console.error('Client made some', err.toString());
                });
            }, 2000);
        }
    });

    channel.on('close', () => {
        console.log(`>> Channel #${idx} closed`);
        if (booBooTimer) {
            clearTimeout(booBooTimer);
        }
    });
}

for (let i = 0; i < createChannelsNum; i++) {
    createChannel(i);
}

process.on('SIGINT', () => {
    console.warn(' ~ Terminating');
    amqpConnection.close();
});
