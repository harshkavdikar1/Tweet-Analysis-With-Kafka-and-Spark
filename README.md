# Tweet Analysis using Kafka and Spark Streaming

Built a real-time analytics dashboard to visualize the trending hashtags and @mentions at a given location by using real time streaming twitter API to get data.

## Installation Guide

#### Download and Install Kafka, Spark, Python and npm. 
<ol>
<li> You can refer to following guide to install kafka. </li>  

> https://towardsdatascience.com/running-zookeeper-kafka-on-windows-10-14fc70dcc771

<li> Spark can be downloaded from following link </li>

> https://spark.apache.org/downloads.html

</ol>

<br>

##  How to run the code.

<ul>
<li> Create kafka topic. </li>
<ul>
<li> You can refer to below link </li>

> https://dzone.com/articles/running-apache-kafka-on-windows-os

<li> Or run following command </li>

> kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter

</ul>

<li> Update conf file with your secret key and access tokens. </li>

<li> Install Python dependencies. </li>

```
 pip install -r requirements.txt
```

<li> Install Node js dependencies. </li>

```
npm install
```

<li> Start Zookeeper </li>

> Open cmd and execute

```
zkserver
```

<li> Start Kafka </li>

> Go to Kafka installation directory. ..\kafka_2.11-2.3.1\bin\windows. Open cmd here and execute following command.

```
kafka-server-start.bat C:\ProgramData\Java\kafka_2.11-2.3.1\config\server.properties
```

<li> Run python file to fetch tweets. </li>

```
python fetch_tweets.py
```

<li> Run python file to analyze tweets. </li>

```
python analyze_tweets.py
```

<li> Start npm server </li>

```
npm start
```

</ul>
<br>

## Technology stack

![stack](stack.png)

</br> 

<table>
<thead>
<tr>
<th>Area</th>
<th>Technology</th>
</tr>
</thead>
<tbody>
	<tr>
		<td>Front-End</td>
		<td> HTML5, Bootstrap, CSS3, Socket.IO, highcharts.js </td>
	</tr>
	<tr>
		<td>Back-End</td>
		<td>Express, Node.js</td>
	</tr>
    <tr>
		<td>Cluster Computing Framework</td>
		<td>Apache Spark (python)</td>
	</tr>
	<tr>
		<td>Message Broker</td>
		<td>Apache kafka</td>
	</tr>
</tbody>
</table>

</br>

## Architecture

</br>

![architecture](architecture.png)

</br>

## How it works
<ol>
    <li>Extract data from Twitter's streaming API and put it into Kakfa topic.</li>
    <li>Spark is listening to this topic, it will read the data from topic, analyze it is using spark streaming and put top 10 trending hashtags and @mentions into another kafka topic.</li>
    <li>Spark Streaming creates DStream whenever it read the data from kafka and analyze it by performing operation like map, filter, updateStateByKey, countByValues and forEachRDD on the RDD and top 10 hashtags and mentions are obtained from RDD using SparkSQL.</li>
    <li>Node.js will pick up the this data from kafka topic on server side and emit it to the socket.</li>
    <li>Socket will push data to user's dashboard which is rendered using highcharts.js in realtime.</li>
    <li>The dashboard is refreshed every 60 secs.</li>
</ol>

</br>

</br>

![hashtags](hashtags.png)

![mentions](mentions.png)
