const Kafka = require("node-rdkafka");
const fs = require("fs");

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

const produceMessagesOnSecondIntervals = async () => {
  // produce 100 messages on 1 second intervals
  let i = 0;
  while (i++ < 100) {
    try {
      if (!producer.isConnected()) {
        await sleep(1000);
        continue;
      }

      const filePath = "vid.mp4"; // Replace with the actual file path
      const fileData = fs.readFileSync(filePath);
      producer.produce(
        // Topic to send the message to
        TOPIC_NAME,
        // optionally we can manually specify a partition for the message
        // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
        null,
        // Message to send. Must be a buffer
        fileData,
        // for keyed messages, we also specify the key - note that this field is optional
        null,
        // you can send a timestamp here. If your broker version supports it,
        // it will get added. Otherwise, we default to 0
        Date.now()
      );
      console.log(`Message sent: vid.mp4`);
    } catch (err) {
      console.error("A problem occurred when sending our message");
      console.error(err);
    }

    await sleep(1000);
  }

  producer.disconnect();
};

produceMessagesOnSecondIntervals();
