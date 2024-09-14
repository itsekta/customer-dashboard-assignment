import app from "./app";
import { runConsumer } from "./kafka/kafkaConsumer";

const PORT = process.env.PORT || 3000;

const startApp = async () => {
  try {
    await runConsumer();

    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Error starting the app:", error);
  }
};

startApp();
