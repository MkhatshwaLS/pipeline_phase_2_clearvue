const axios = require("axios");

const PAYMENT_WEBHOOK_URL = "http://localhost:3000/api/payments/webhook";

const samplePayments = [
  {
    paymentId: "pay_" + Math.random().toString(36).substr(2, 9),
    amount: 150.75,
    currency: "USD",
    customerId: "cust_12345",
    orderId: "order_67890",
    paymentMethod: "credit_card",
    status: "completed",
    timestamp: new Date().toISOString(),
  },
  // Add more sample payments...
];

async function simulatePayment() {
  try {
    const payment = samplePayments[0];
    // Randomize amount for each simulation
    payment.amount = (Math.random() * 1000).toFixed(2);
    payment.paymentId = "pay_" + Math.random().toString(36).substr(2, 9);
    payment.timestamp = new Date().toISOString();

    const response = await axios.post(PAYMENT_WEBHOOK_URL, payment);
    console.log(`Simulated payment ${payment.paymentId}: ${response.status}`);
  } catch (error) {
    console.error("Simulation error:", error.response?.data || error.message);
  }
}

// Simulate a payment every 5 seconds
setInterval(simulatePayment, 5000);
console.log("Payment simulation started...");
