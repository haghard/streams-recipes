# streams-recipes
ScalazStream, AkkaStream, Fs2 recipes
==================================

https://piotrminkowski.wordpress.com/2017/08/29/visualizing-jenkins-pipeline-results-in-grafana/

 
Run grafana + graphite docker image   

``` 
    docker run -it -p 80:80 -p 8125:8125/udp -p 8126:8126 kamon/grafana_graphite
```

### Akka-streams links ###
##Main goal: getting data across async boundary with non-blocking back pressure ##

http://allaboutscala.com/scala-frameworks/akka/
http://blog.akka.io/streams/2016/07/30/mastering-graph-stage-part-1
https://doc.akka.io/docs/akka/current/stream/stream-graphs.html#bidirectional-flows

http://blog.colinbreck.com/partitioning-akka-streams-for-scalability-and-high-availability/
https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-ii/

https://softwaremill.com/implementing-a-custom-akka-streams-graph-stage/

https://mikołak.net/blog/2017/akka-streams-libgdx-7.html
http://leaks.wanari.com/2017/11/07/ray-tracing-akka-part-4/
https://blog.softwaremill.com/akka-streams-pitfalls-to-avoid-part-2-f93e60746c58
https://blog.scalac.io/2017/04/25/akka-streams-graph-stage.html
https://markatta.com/codemonkey/posts/chat-with-akka-http-websockets/

https://softwaremill.com/interval-based-rate-limiter/
https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

https://efekahraman.github.io/2019/01/session-windows-in-akka-streams

https://softwaremill.com/windowing-data-in-akka-streams/
https://github.com/IBM/db2-event-store-akka-streams
http://jesseyates.com/2019/04/07/just-right-parallelism-in-akka-streams.html


https://engineering.prezi.com/prox-part-2-akka-streams-with-cats-effect-f63c28199cad
http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/


https://skillsmatter.com/skillscasts/6869-workshop-end-to-end-asynchronous-back-pressure-with-akka-streams


Examples using Akka HTTP with Streaming
https://github.com/ktoso/akka-http-streaming-response-examples

## akka.stream.contrib
https://github.com/akka/akka-stream-contrib/tree/master/src/main/scala/akka/stream/contrib

## Integrating streams and actors
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iii/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/

https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-i/
https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-ii/

https://blog.colinbreck.com/backoff-and-retry-error-handling-for-akka-streams/


http
https://github.com/hseeberger/accessus.git



https://www.youtube.com/watch?v=MzosGtjJdPg
https://blog.colinbreck.com/partitioning-akka-streams-for-scalability-and-high-availability/
https://blog.colinbreck.com/partitioning-akka-streams-to-maximize-throughput/

https://blog.colinbreck.com/patterns-for-streaming-measurement-data-with-akka-streams/
(The concepts of batching measurements, decomposing measurements, rate-limiting requests, throttling requests, performing tasks concurrently, and so on)

https://blog.colinbreck.com/akka-streams-a-motivating-example/

https://blog.colinbreck.com/calling-blocking-code-there-is-no-free-lunch/
https://blog.colinbreck.com/performance-considerations-for-akka-debug-logging/

https://blog.colinbreck.com/considering-time-in-a-streaming-data-system/

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


### TumblingWindow, Sliding, SessionInactivity and SessionValue windows ###


 The following timing windows:

    Tumbling window - windows that have a fixed (time) size and do not overlap. In this case time is divided into non-overlapping parts and each data element belongs to a single window.
    Sliding windows - windows parameterized by length and step. These windows overlap, and each data element can belong to multiple windows.
    Inactivity based session windows - windows defining session on the inactivity period (session completes if no data arrives within inactivity data interval).
    Value based session windows - windows defining session based on the message parameter (in addition session in this case will complete in the case of inactivity)

https://www.lightbend.com/blog/comparing-runtimes-in-cloudflow-akka-streams-vs-apache-flink

https://flink.apache.org/news/2015/12/04/Introducing-windows.html
https://efekahraman.github.io/2019/01/session-windows-in-akka-streams
https://softwaremill.com/windowing-data-in-akka-streams/
https://blog.kunicki.org/blog/2016/07/20/implementing-a-custom-akka-streams-graph-stage/
https://github.com/efekahraman/akka-streams-session-window
https://softwaremill.com/implementing-a-custom-akka-streams-graph-stage/
https://blog.softwaremill.com/painlessly-passing-message-context-through-akka-streams-1615b11efc2c



## Akka streams links ##

https://akka.io/blog/article/2016/08/25/simple-sink-source-with-graphstage


http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/
https://blog.softwaremill.com/painlessly-passing-message-context-through-akka-streams-1615b11efc2c
https://github.com/calvinlfer/Akka-Streams-custom-stream-processing-examples
https://www.infoq.com/presentations/squbs/
https://github.com/gosubpl/akka-online.git
https://aleksandarskrbic.github.io/power-of-akka-streams/
https://engineering.prezi.com/prox-part-2-akka-streams-with-cats-effect-f63c28199cad


https://blog.softwaremill.com/painlessly-passing-message-context-through-akka-streams-1615b11efc2c
https://blog.colinbreck.com/maximizing-throughput-for-akka-streams/
https://blog.colinbreck.com/partitioning-akka-streams-to-maximize-throughput/
the responding talk https://youtu.be/MzosGtjJdPg
 
 
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iii/
https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/
https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-i/
https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-ii/

https://medium.com/swlh/akka-stream-a-walkthrough-from-simple-source-to-parallel-execution-6a7ec24d07d8
https://softwaremill.com/windowing-data-in-akka-streams/


https://doc.akka.io/docs/akka/current/stream/stream-customize.html
https://doc.akka.io/docs/akka/current/stream/stream-customize.html#rate-decoupled-operators
https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

https://github.com/svezfaz/akka-backpressure-scala-central-talk.git

Stefano Bonetti - Monitoring Akka Streams: https://www.youtube.com/watch?v=4s1YzgrRR2A


netstat -na | grep ${PORT}

## FS2 links ##
FS2 is pull based streaming. Basically it means that downstream decides when to pull from upstream.    

https://fs2.io/documentation.html
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
https://www.youtube.com/watch?v=oluPEFlXumw

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
        

Resource management 
https://medium.com/@bszwej/composable-resource-management-in-scala-ce902bda48b2

Fabio Labella—How do Fibers Work? A Peek Under the Hood
https://www.youtube.com/watch?v=x5_MmZVLiSM&list=PL9KXdMOfekva4SJJqLDXQ73nMXt8-3pz7&index=4

  
