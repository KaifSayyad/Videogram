const Kafka = require("node-rdkafka");
<<<<<<< HEAD
const path = require("path");
const cv = require("opencv4nodejs");
=======
const { spawn } = require("child_process");
const path = require("path");
>>>>>>> c8fa8d2e8bfe8b6c3cb44abccd5d0ff3595feb5a

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

<<<<<<< HEAD
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
=======
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
>>>>>>> c8fa8d2e8bfe8b6c3cb44abccd5d0ff3595feb5a
        null,
        Date.now()
      );
      console.log(`Sent video frame to Kafka`);
<<<<<<< HEAD

      cv.imshow("Video", frame);
      const key = cv.waitKey(1);
      if (key === 27) { // Press ESC to exit
        break;
      }

      await sleep(1000);
=======
>>>>>>> c8fa8d2e8bfe8b6c3cb44abccd5d0ff3595feb5a
    } catch (err) {
      console.error("A problem occurred when sending our message");
      console.error(err);
    }
<<<<<<< HEAD
  }

  ffmpeg.release();
  producer.disconnect();
};

captureAndDisplayVideo();
=======
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
>>>>>>> c8fa8d2e8bfe8b6c3cb44abccd5d0ff3595feb5a
