import { Kafka, Partitioners } from "kafkajs";
import { Message } from "../types/messageTypes";

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

export const runProducer = async (message: Message): Promise<void> => {
  try {
    await producer.connect();
    console.log("Producer connected");

    await producer.send({
      topic: "test",
      messages: [{ value: JSON.stringify(message) }],
    });

    console.log("Message sent successfully");
  } catch (error) {
    console.error("Error sending message:", error);
  } finally {
    await producer.disconnect();
    console.log("Producer disconnected");
  }
};
