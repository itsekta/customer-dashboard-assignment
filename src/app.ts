import express from "express";
import cors from "cors";
import messageRoutes from "./routes/messageRoutes";

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api", messageRoutes);

export default app;
