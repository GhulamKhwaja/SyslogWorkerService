const axios = require("axios");
const startWorker = require("./queue"); // This is now the Kafka consumer module
const https = require("https");

const agent = new https.Agent({
  rejectUnauthorized: false
});

async function processJob(job) {
  console.log("Processing job for", job.ip);

  
  try {
    await axios.post("https://configbackup:3002/adhocRequest", job,
    { httpsAgent: agent }
    );
    console.log("Backup triggered for", job.deviceIp);
  } catch (err) {
    console.error("Backup failed for", job.deviceIp, ":", err.message);

    // Throw error so Kafka DOES NOT commit offset
    // This makes the message retry later
    throw err;
  }
}

// Start Kafka worker
(async () => {
  try {
    await startWorker(processJob);
  } catch (err) {
    console.error("Worker failed to start:", err);
    process.exit(1);
  }
})();
