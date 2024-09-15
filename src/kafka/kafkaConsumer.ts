import { Kafka } from "kafkajs";
import { Message } from "../types/messageTypes";
import { runProducer } from "./kafkaProducer";
import formatTime from "../utils/timeFormat";
import formatDate from "../utils/dateFormat";
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });

let liveData: Message | null = null;
let messageHistory: Message[] = [];

export const runConsumer = async () => {
  try {
    // Dummy data from producer
    const messages = [
      {
        store_id: 10,
        customers_in: 2,
        customers_out: 3,
        time_stamp: formatTime(new Date()),
        date: new Date().toISOString(),
      },
      {
        store_id: 10,
        customers_in: 0,
        customers_out: 1,
        time_stamp: formatTime(new Date()),
        date: new Date().toISOString(),
      },
      {
        store_id: 10,
        customers_in: 2,
        customers_out: 0,
        time_stamp: formatTime(new Date()),
        date: new Date().toISOString(),
      },
    ];

    for (const message of messages) {
      await runProducer(message);
    }

    await consumer.connect();
    console.log("Consumer connected");

    await consumer.subscribe({ topic: "test", fromBeginning: true });
    console.log("Consumer subscribed to topic");

    await consumer.run({
      eachMessage: async ({ message }) => {
        const receivedMessage = message.value?.toString();
        if (receivedMessage) {
          try {
            const parsedMessage: Message = JSON.parse(receivedMessage);

            // Update liveData with the most recent message
            liveData = parsedMessage;

            // Update messageHistory
            messageHistory.push(parsedMessage);
          } catch (error) {
            console.error("Error parsing message:", error);
          }
        }
      },
    });
  } catch (error) {
    console.error("Error consuming messages:", error);
  }
};

export const getLiveData = () => liveData;

export const getMessageHistory = () => {
  const updateHistory = messageHistory.map((msg) => {
    return {
      date: formatDate(msg.date),
      customers_in: msg.customers_in,
      customers_out: msg.customers_out,
    };
  });

  return updateHistory;
};
