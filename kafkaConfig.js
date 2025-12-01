const { Kafka } = require("kafkajs");
require("dotenv").config();

const kafka = new Kafka({
  clientId: "airline-data-warehouse",
  brokers: [process.env.KAFKA_BROKERS],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "flight-status-group" });

class KafkaService {
  static async connect() {
    await producer.connect();
    await consumer.connect();
    console.log("Connected to Kafka");
  }

  static async sendFlightUpdate(flightData) {
    try {
      await producer.send({
        topic: "flight-status-updates",
        messages: [
          {
            key: flightData.flight_key,
            value: JSON.stringify(flightData),
          },
        ],
      });
      console.log(`Sent flight update for ${flightData.flight_key}`);
    } catch (error) {
      console.error("Error sending to Kafka:", error);
    }
  }

  static async listenForFlightDelays(callback) {
    await consumer.subscribe({
      topic: "flight-status-updates",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const flightData = JSON.parse(message.value.toString());
          await callback(flightData);
        } catch (error) {
          console.error("Error processing Kafka message:", error);
        }
      },
    });
  }
}

module.exports = { KafkaService, producer, consumer };
