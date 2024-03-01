const Kafka = require("node-rdkafka");
const { spawn } = require("child_process");
const ffmpegPath = require("ffmpeg-static");

const TOPIC_NAME_CONSUMER_1 = "consumer1_topic";
const TOPIC_NAME_CONSUMER_2 = "consumer2_topic";

const consumer1 = new Kafka.KafkaConsumer({
  "group.id": "my-group-1",
  "metadata.broker.list": "kafka-broker:9092",
  "security.protocol": "ssl",
  "ssl.key.location": "service.key",
  "ssl.certificate.location": "service.cert",
  "ssl.ca.location": "ca.pem",
});

consumer1.connect();

const produceVideoFramesToConsumer2 = async () => {
  const ffmpeg = spawn(ffmpegPath, [
    "-f", "dshow",
    "-i", "video=Integrated Webcam",
    "-f", "rawvideo",
    "-pix_fmt", "rgb24",
    "pipe:1",
  ]);

  const producer = new Kafka.Producer({
    "metadata.broker.list": "kafka-broker:9092",
    "security.protocol": "ssl",
    "ssl.key.location": "service.key",
    "ssl.certificate.location": "service.cert",
    "ssl.ca.location": "ca.pem",
    dr_cb: true
  });

  producer.connect();

  ffmpeg.stdout.on("data", async (data) => {
    try {
      producer.produce(
        TOPIC_NAME_CONSUMER_2,
        null,
        data,
        null,
        Date.now()
      );
    } catch (err) {
      console.error("Error sending video data to Kafka:", err);
    }
  });

  // Handle process termination
  process.on('SIGINT', () => {
    producer.disconnect();
    ffmpeg.kill();
    process.exit();
  });
};

produceVideoFramesToConsumer2();
