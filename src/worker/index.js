const amqp = require('amqplib')
const config = require('../config');

const assertQueueOptions = { durable: true }
const consumeQueueOptions = { noAck: false } // ensures that the msg was processed by the consumer
const connectConfig = { clientProperties : {connection_name: 'NodeJs-Consumer'}};
const { uri, workQueue } = config

const processHeavyTask = msg => {
    console.log(msg.content.toString())
}

const assertAndConsumeQueue = async (channel) => {
    console.log('Worker is running! Waiting for new messages...')
  
    const ackMsg = async (msg) => {
        await processHeavyTask(msg)
        await channel.ack(msg)
    }

    await channel.assertQueue(workQueue, assertQueueOptions)
    await channel.prefetch(1) // only sends msg to free consumer
    await channel.consume(workQueue, ackMsg, consumeQueueOptions)
}

const listenToQueue = async () => {
    const connection = await amqp.connect(uri, connectConfig);
    const channel = await connection.createChannel();
    await assertAndConsumeQueue(channel);
} 

listenToQueue()