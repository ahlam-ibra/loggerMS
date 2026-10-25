import amqp from "amqplib";
import { config } from "./config.js";

// 1\ connect to RabbitMQ server
// 2\ create a new channel on the connection
// 3\ create the exchange
// 4\ publish the message with the routing key (Direct Exchange)

export class Producer {
    // has one field which is channel
    channel;

    async createChannel() {
        const connection = await amqp.connect(config.rabbitMQ.url);
        this.channel = await connection.createChannel();
    }

    async publishMessage(routingKey, message) {
        if (!this.channel) {
            // to ensure that we are working with the same single channel
            await this.createChannel();
        }

        const exchangeName = config.rabbitMQ.exchangeName;
        const exchangeType = config.rabbitMQ.exchangeType;
        await this.channel.assertExchange(exchangeName, exchangeType);

        const logDetails = {
            logType: routingKey,
            message,
            dateTime: new Date(),
        };

        await this.channel.publish(exchangeName, routingKey, Buffer.from(JSON.stringify(logDetails)));

        console.log(`The message ${message} is sent to exchange ${exchangeName}`);
    }
}