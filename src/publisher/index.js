const amqp = require('amqplib');
const config = require('../config');

const assertQueueOptions = { durable: true };
const sendToQueueOptions = { persistent: true };
const { uri, workQueue } = config;

// open connection, create and return channel
const openConnectionAndCreateChannel = async () => {
    const connection = await amqp.connect(uri);
    const channel = await connection.createChannel();

    return channel;
}

// create Buffer for data
const assertAndSendToQueue = async (message, channel) => {
    const bufferedData = Buffer.from(message)

    await channel.assertQueue(workQueue, assertQueueOptions);
    await channel.sendToQueue(workQueue, bufferedData, sendToQueueOptions);
}

// send message before close channel
const sendMessage = async (message) => {
    const channel = await openConnectionAndCreateChannel();
    await assertAndSendToQueue(message, channel);
    await channel.close();
} 

const start = async (message) => {
    let element;

    for (let index = 0; index < 100; index++) {
        element = ''
        element = `${index} - ${message}`;
        await sendMessage(element);

        console.log(`The message ${element} send sucess`);
    }

    process.exit(0);
}

//start("teste")
module.exports = start