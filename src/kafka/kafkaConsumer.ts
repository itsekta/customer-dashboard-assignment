import { Kafka } from "kafkajs";
import { Message } from "../types/messageTypes";
import { runProducer } from "./kafkaProducer";
import formatTime from "../utils/timeFormat";
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });

let liveData: Message[] = [];
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
      },
      {
        store_id: 10,
        customers_in: 0,
        customers_out: 1,
        time_stamp: formatTime(new Date()),
      },
      {
        store_id: 10,
        customers_in: 2,
        customers_out: 0,
        time_stamp: formatTime(new Date()),
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

            // Add to live data
            liveData.push(parsedMessage);

            // Add to historical data (24-hour window)
            // const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
            // messageHistory = messageHistory.filter(
            //   (msg) => new Date(msg.time_stamp) > oneDayAgo
            // );
            // messageHistory.push(parsedMessage);
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
  const groupedHistory: {
    [hour: string]: { customers_in: number; customers_out: number };
  } = {};

  messageHistory.forEach((msg) => {
    const hour = new Date().toISOString().slice(0, 13);
    // console.log("Hour",hour);

    if (!groupedHistory[hour]) {
      groupedHistory[hour] = { customers_in: 0, customers_out: 0 };
    }
    groupedHistory[hour].customers_in += msg.customers_in;
    groupedHistory[hour].customers_out += msg.customers_out;
  });

  return Object.keys(groupedHistory).map((hour) => ({
    hour,
    ...groupedHistory[hour],
  }));
};
