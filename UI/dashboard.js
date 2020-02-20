var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafka = require('kafka-node');
var port = 8081;

var Consumer = kafka.Consumer,
 client = new kafka.KafkaClient("localhost:9092"),
 consumer = new Consumer(
 client, [ { topic: 'processedtweets', partition: 0 } ], { autoCommit: false });

app.get('/', function(req, res){
    res.sendfile('index.html');
});

io = io.on('connection', function(socket){
    console.log('a user connected');
    socket.on('disconnect', function(){
        console.log('user disconnected');
    });
});

consumer = consumer.on('message', function(message) {
    // console.log(message.value);
    io.emit("message", message.value);
});

http.listen(port, function(){
    console.log("Running on port " + port)
});