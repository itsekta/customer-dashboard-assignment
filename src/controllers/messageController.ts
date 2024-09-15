import { Request, Response } from "express";
import { runProducer } from "../kafka/kafkaProducer";
import { getLiveData, getMessageHistory } from "../kafka/kafkaConsumer";
import { Message } from "../types/messageTypes";
import formatTime from "../utils/timeFormat";

// Add new message
export const addMessage = async (req: Request, res: Response) => {
  try {
    // console.log("Data received",req.body);
    const { store_id, customers_in, customers_out } = req.body;

    if (
      !store_id ||
      customers_in === undefined ||
      customers_out === undefined
    ) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const message: Message = {
      store_id,
      customers_in,
      customers_out,
      time_stamp: formatTime(new Date()),
    };
    await runProducer(message);
    res.status(200).json({ message: "Message added successfully" });
  } catch (error) {
    console.error("Error adding message:", error);
    res.status(500).json({ error: "Internal server error" });
  }
};

// Get live data
export const fetchLiveData = (req: Request, res: Response) => {
  res.json(getLiveData());
};

// Get historical data
export const fetchHistoricalData = (req: Request, res: Response) => {
  res.json(getMessageHistory());
};
