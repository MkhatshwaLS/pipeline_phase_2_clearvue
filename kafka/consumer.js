// consumer.js
import { Kafka } from "kafkajs";
import mongoose from "mongoose";

// MongoDB config
await mongoose.connect("mongodb://localhost:27017/clearvue");

const paymentSchema = new mongoose.Schema(
  {
    transactionId: String,
    userId: String,
    amount: Number,
    status: String,
    timestamp: Date,
  },
  { strict: false }
);
const PaymentModel = mongoose.model("Payment", paymentSchema);

const kafka = new Kafka({
  clientId: "payment-processor",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "payment-group" });

async function consumePayments() {
  await consumer.connect();
  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payment = JSON.parse(message.value.toString());
        await PaymentModel.create(payment);
        console.log("💾 Inserted payment:", payment);
      } catch (err) {
        console.error("❌ Failed to insert payment:", err);
      }
    },
  });
}

consumePayments();
