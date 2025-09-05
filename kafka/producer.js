// producer.js
import { Kafka } from "kafkajs";
import fs from "fs";
import csvParser from "csv-parser";

// Kafka config
const kafka = new Kafka({
  clientId: "payment-simulator",
  brokers: ["localhost:9092"], // update if Kafka is remote
});

const producer = kafka.producer();

async function producePayments(csvFile) {
  await producer.connect();

  const payments = [];
  fs.createReadStream(csvFile)
    .pipe(csvParser())
    .on("data", (row) => {
      payments.push(row);
    })
    .on("end", async () => {
      console.log(`✅ Loaded ${payments.length} payments from CSV`);

      for (const payment of payments) {
        await producer.send({
          topic: "payments",
          messages: [{ value: JSON.stringify(payment) }],
        });
        console.log("📤 Sent payment:", payment);

        await new Promise((resolve) => setTimeout(resolve, 2000)); // wait 2s
      }

      await producer.disconnect();
      console.log("✅ All payments sent.");
    });
}

producePayments("./payments.csv");
