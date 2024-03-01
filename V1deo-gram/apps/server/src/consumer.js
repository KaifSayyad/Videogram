const Kafka = require("node-rdkafka");
const { spawn } = require("child_process");

const TOPIC_NAME = "demo_topic";

// Kafka producer configuration
const producer = new Kafka.Producer({
  "metadata.broker.list": "kafka-17907454-v1deo-gram.a.aivencloud.com:26681",
  "dr_cb": true, // Enable delivery report callback
  "security.protocol": "ssl",
  "ssl.key.location": "service.key",
  "ssl.certificate.location": "service.cert",
  "ssl.ca.location": "ca.pem",
});

// Connect the producer to the Kafka cluster
producer.connect();

// Specify the full path to the ffmpeg executable
const ffmpegPath = "C:/Program Files/ffmpeg/bin/ffmpeg.exe"; // Replace with the actual path

const videoStream = spawn(ffmpegPath, [
  "-f", "dshow",
  "-i", "video=Integrated Camera",
  "-f", "mpegts",
  "-codec:v", "mpeg1video",
  "-s", "640x480",
  "-b:v", "800k",
  "-bf", "0",
  "-muxdelay", "0.001",
  "-"
]);

videoStream.stdout.on("data", (data) => {
  try {
    // Produce message to Kafka
    producer.produce(
      TOPIC_NAME,
      null,
      Buffer.from(data), // Convert data to buffer
      null,
      Date.now()
    );
    console.log("Sent video data to Kafka.");
  } catch (err) {
    console.error("Error sending video data to Kafka:", err);
  }
});

// Handle delivery report callback
producer.on('delivery-report', function(err, report) {
  if (err) {
    console.error('Error producing message:', err);
  } else {
    console.log('Message delivered to topic:', report.topic);
  }
});

// Handle process termination
process.on('SIGINT', () => {
  producer.disconnect();
  videoStream.kill();
  process.exit();
});
