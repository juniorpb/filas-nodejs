const rabbitConfig = {
    uri: process.env.rabbbitUri || 'amqp://localhost',
    workQueue: process.env.workQueue || 'workQueue',
}
module.exports = rabbitConfig