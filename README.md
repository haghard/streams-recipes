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


## Akka streams links ##
http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/


https://github.com/svezfaz/akka-backpressure-scala-central-talk.git

netstat -na | grep ${PORT}

## FS2 links ##
FS2 is pull based streaming. Basically it means that downstream decides when to pull from upstream.    

https://github.com/functional-streams-for-scala/fs2
https://lunatech.com/blog/WCl5OikAAIrvQCoc/functional-io-with-fs2-streams
https://underscore.io/blog/posts/2018/03/20/fs2.html
https://gist.github.com/narench/1bead6045874883fc227026e458333f1
https://fs2.io/concurrency-primitives.html
https://www.beyondthelines.net/programming/streaming-patterns-with-fs2
https://medium.freecodecamp.org/a-streaming-library-with-a-superpower-fs2-and-functional-programming-6f602079f70a
https://github.com/gvolpe/advanced-http4s/blob/master/src/main/scala/com/github/gvolpe/fs2/PubSub.scala
https://github.com/profunktor/tutorials.git

fs2+scalaJs https://tech.ovoenergy.com/frontend-development-with-scala-js/

## FS2 videos ##
https://www.youtube.com/watch?v=B1wb4fIdtn4
https://www.youtube.com/watch?v=x3GLwl1FxcA
https://www.youtube.com/watch?v=mStYwML3JZk + https://speakerdeck.com/mpilquist/fs3-evolving-a-streaming-platform

Profunktor:
    https://www.youtube.com/watch?v=FWYXqYQWAc0
    https://www.youtube.com/watch?v=uuocHqdnoS0

A practical look at fs2 Streams Bracket:
  https://youtu.be/5TR89KzPaJ4?list=PLbZ2T3O9BuvczX5j03bWMrMFzK5OAs9mZ

CRASH COURSE FS2 - Łukasz Byczyński | Scalar 2019 
https://www.youtube.com/watch?v=tbShO8eIXbI    


Type-Level DSLs // Typeclass induction
https://youtu.be/Nm4OIhjjA2o
https://gist.github.com/aaronlevin/d3911ba50d8f5253c85d2c726c63947b


Validations - https://blog.softwaremill.com/38-lines-of-code-towards-better-data-validation-in-scala-c933e5a88f76
Traverse HList - 
    https://blog.free2move.com/shapeless-hlists-and-how-to-traverse-them/
    https://olegpy.com/traversing-hlists/