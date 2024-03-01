const Kafka = require("node-rdkafka");
const { spawn } = require("child_process");
const path = require("path");

const TOPIC_NAME = "demo_topic";

const producer = new Kafka.Producer({
  "metadata.broker.list": "kafka-17907454-v1deo-gram.a.aivencloud.com:26681",
  "security.protocol": "ssl",
  "ssl.key.location": "service.key",
  "ssl.certificate.location": "service.cert",
  "ssl.ca.location": "ca.pem",
  dr_cb: true
});

producer.connect();

const sleep = async (timeInMs) =>
  await new Promise((resolve) => setTimeout(resolve, timeInMs));

const ffmpegPath = "C:/Program Files/ffmpeg/bin/ffmpeg.exe"; // Replace with the actual path to ffmpeg.exe

const produceVideoOnSecondIntervals = async () => {
  // produce video frames on 1 second intervals
  const ffmpeg = spawn(ffmpegPath, [
    "-f", "dshow",
    "-i", "video=Integrated Camera",
    "-f", "rawvideo",
    "-pix_fmt", "rgb24",
    "-"
  ]);

  ffmpeg.stdout.on("data", async (data) => {
    try {
      if (!producer.isConnected()) {
        await sleep(1000);
        return;
      }

      producer.produce(
        TOPIC_NAME,
        null,
        data,
        null,
        Date.now()
      );
      console.log(`Sent video frame to Kafka`);
    } catch (err) {
      console.error("A problem occurred when sending our message");
      console.error(err);
    }
  });

  ffmpeg.stderr.on("data", (data) => {
    console.error(`ffmpeg stderr: ${data}`);
  });

  ffmpeg.on("close", (code) => {
    console.log(`ffmpeg process exited with code ${code}`);
  });

  // Handle process termination
  process.on('SIGINT', () => {
    ffmpeg.kill();
    producer.disconnect();
    process.exit();
  });
};

produceVideoOnSecondIntervals();