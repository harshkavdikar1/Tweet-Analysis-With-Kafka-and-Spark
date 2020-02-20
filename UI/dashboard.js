var kafka = require('kafka-node');
var Consumer = kafka.Consumer,
 client = new kafka.KafkaClient("localhost:9092"),
 consumer = new Consumer(
 client, [ { topic: 'processedtweets', partition: 0 } ], { autoCommit: false });

consumer.on('message', function (message) {
console.log(message);
});
 