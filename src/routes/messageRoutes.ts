import { Router } from "express";
import {
  addMessage,
  fetchLiveData,
  fetchHistoricalData,
} from "../controllers/messageController";

const router = Router();

router.post("/add-message", addMessage);
router.get("/live-data", fetchLiveData);
router.get("/history-data", fetchHistoricalData);

export default router;
