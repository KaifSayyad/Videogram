const Kafka = require("node-rdkafka");
const path = require("path");
const cv = require("opencv4nodejs");

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

const ffmpegPath = "C:/ffmpeg/bin/ffmpeg.exe"; // Replace with the actual path to ffmpeg.exe

const captureAndDisplayVideo = async () => {
  const ffmpeg = cv.VideoCapture(0);

  while (true) {
    try {
      const frame = ffmpeg.read();
      if (frame.empty) {
        break;
      }

      const image = cv.imencode('.jpg', frame).toString('base64');
      producer.produce(
        TOPIC_NAME,
        null,
        Buffer.from(image),
        null,
        Date.now()
      );
      console.log(`Sent video frame to Kafka`);

      cv.imshow("Video", frame);
      const key = cv.waitKey(1);
      if (key === 27) { // Press ESC to exit
        break;
      }

      await sleep(1000);
    } catch (err) {
      console.error("A problem occurred when sending our message");
      console.error(err);
    }
  }

  ffmpeg.release();
  producer.disconnect();
};

captureAndDisplayVideo();
