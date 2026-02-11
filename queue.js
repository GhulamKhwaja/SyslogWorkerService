const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "config-worker",
  brokers: ["kafka:9092"] // Change if needed
});

/**
 * Start Kafka consumer worker
 * @param {Function} handler - async function that processes a job
 */
async function startWorker(handler) {
  const consumer = kafka.consumer({
    groupId: "config-jobs-group" // Workers in same group share load
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: "config_jobs",
    fromBeginning: false // Only new jobs
  });

  console.log("Kafka worker connected and listening...");

  await consumer.run({
    autoCommit: false, // We control when offset is committed
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const job = JSON.parse(message.value.toString());

        await handler(job); // process job

        // Commit offset AFTER success (similar to ack)
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (Number(message.offset) + 1).toString()
          }
        ]);
      } catch (err) {
        console.error("Job processing failed:", err);

        // Do NOT commit offset → Kafka will retry
      }
    }
  });
}

module.exports = startWorker;
