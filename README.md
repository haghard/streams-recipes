# streams-recipes
ScalazStream, AkkaStream, Fs2 recipes
==================================

https://piotrminkowski.wordpress.com/2017/08/29/visualizing-jenkins-pipeline-results-in-grafana/

 
Run grafana + graphite docker image   

``` 
    docker run -it -p 80:80 -p 8125:8125/udp -p 8126:8126 kamon/grafana_graphite
```

### Useful links ###

http://allaboutscala.com/scala-frameworks/akka/
http://blog.akka.io/streams/2016/07/30/mastering-graph-stage-part-1
https://doc.akka.io/docs/akka/current/stream/stream-graphs.html#bidirectional-flows
Rethinking Streaming Workloads with Akka Streams: Part II https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-ii/

### How to set up grafana_graphite ### 

http://stackoverflow.com/questions/32459582/how-to-set-up-statsd-along-with-grafana-graphite-as-backend-for-kamon


If you set up a new datasource with the following properties:

Name: graphite
Default: checked
Type: Graphite
URL: http://localhost:8000
Access: proxy
You should then have a datasource that points to the Graphite metric data within the Docker container.

Note - the default username/password for the Grafana UI is admin/admin.



### windows ###

https://flink.apache.org/news/2015/12/04/Introducing-windows.html
https://efekahraman.github.io/2019/01/session-windows-in-akka-streams
https://softwaremill.com/windowing-data-in-akka-streams/
https://blog.kunicki.org/blog/2016/07/20/implementing-a-custom-akka-streams-graph-stage/
https://github.com/efekahraman/akka-streams-session-window


https://github.com/svezfaz/akka-backpressure-scala-central-talk.git

netstat -na | grep ${PORT}

## fs2 ##
https://lunatech.com/blog/WCl5OikAAIrvQCoc/functional-io-with-fs2-streams
https://underscore.io/blog/posts/2018/03/20/fs2.html
https://gist.github.com/narench/1bead6045874883fc227026e458333f1
https://fs2.io/concurrency-primitives.html
https://www.beyondthelines.net/programming/streaming-patterns-with-fs2/

https://github.com/profunktor/tutorials.git