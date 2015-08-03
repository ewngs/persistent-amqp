'use strict';

const mq = require('..')();

process.on('SIGINT', () => {
    console.warn(' ~ Got SIGINT, terminating');
    if (mq.isOpen()) {
        mq.terminate();
    }
});

mq.on('open', () => {
    console.log('> got channel opened event');
    mq.assertQueue('test1').then(queue => {
        console.log('> queue exists/created:', queue);
    });
});

mq.on('close', () => {
    console.log('> got channel closed event');
});
