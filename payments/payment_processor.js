const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

async function startPaymentProcessor() {
  // Kafka consumer
  const kafka = new Kafka({
    clientId: "payment-processor",
    brokers: [process.env.KAFKA_BROKER || "kafka:29092"],
  });

  const consumer = kafka.consumer({ groupId: "payment-processor-group" });

  // MongoDB connection
  const client = new MongoClient(
    process.env.MONGODB_URI || "mongodb://mongodb:27017"
  );
  await client.connect();
  const db = client.db("clearvue_db");
  const paymentsCollection = db.collection("payments");

  // Process payment events
  await consumer.connect();
  await consumer.subscribe({ topic: "payment-events", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payment = JSON.parse(message.value.toString());

        // Add financial period metadata
        payment.financialYear = calculateFinancialYear(payment.timestamp);
        payment.financialQuarter = calculateFinancialQuarter(payment.timestamp);

        // Store in MongoDB
        await paymentsCollection.insertOne(payment);

        console.log(`Stored payment ${payment.paymentId} in database`);
      } catch (error) {
        console.error("Error processing payment:", error);
      }
    },
  });
}

function calculateFinancialYear(date) {
  // Implement ClearVue's financial year logic
  const eventDate = new Date(date);
  const year = eventDate.getFullYear();
  const august = new Date(year, 7, 31); // August 31
  const day = august.getDay();
  const lastSaturday = new Date(august);
  lastSaturday.setDate(31 - ((day + 1) % 7));

  return eventDate >= lastSaturday ? `FY${year + 1}` : `FY${year}`;
}

function calculateFinancialQuarter(date) {
  // Implement quarter calculation based on financial year
  const eventDate = new Date(date);
  const month = eventDate.getMonth();

  if (month >= 8 || month < 2) return "Q1"; // Sep-Nov
  if (month >= 2 && month < 5) return "Q2"; // Dec-Feb
  if (month >= 5 && month < 8) return "Q3"; // Mar-May
  return "Q4"; // Jun-Aug
}

startPaymentProcessor().catch(console.error);
