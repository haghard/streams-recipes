package graph

import quiver._

//https://bintray.com/oncue/releases/quiver/view
object Graph {

  val nil = empty[Int, Char, String]

  val root = empty[Int, Char, Unit]

  //A graph with one node identified as 1, labeled a:
  val a = nil & Context(Vector(), 1, 'a', Vector())
  //Here is a graph with two nodes, a and b, and one edge a -> b:
  val e = a & Context(Vector("a -> b" -> 1), 2, 'b', Vector())

  //same
  val graph = safeMkGraph(
    Seq(LNode(1, 'a'), LNode(2, 'b'), LNode(3, 'c'), LNode(4, 'e')),
    Seq(LEdge(1, 2, "a -> b"), LEdge(1, 3, "a -> c"), LEdge(3, 4, "c -> e")))

  val airports = safeMkGraph(
    Seq(LNode(1, "SEA"),
      LNode(2, "SFO"),
      LNode(3, "LAX"),
      LNode(4, "DFW"),
      LNode(5, "IAD"),
      LNode(6, "JFK")),
    Seq(LEdge(1, 2, 45),
      LEdge(2, 3, 28),
      LEdge(5, 4, 31),
      LEdge(6, 5, 12),
      LEdge(6, 1, 22),
      LEdge(3, 6, 61),
      LEdge(4, 2, 25))
  )
  //from, to, delay

  airports dfs Seq(2)

  //subgraph
  airports.decomp(1).rest

  airports.select(_.label == "SFO")
  //Context(Vector((25,4)),2,SFO,Vector((28,3))) in 4 out 3

  airports.decompAny.rest

  airports degree 2

  graph neighbors 1

  //start to search from 1
  graph dfs Seq(1)

  val tree = graph dff Seq(3)

  //tree.head.drawTree

  val graph1 = buildGraph[Int, Char, String](
    Seq(Context(Vector(), 1, 'a', Vector("a -> a" -> 1)),
      Context(Vector("a -> b" -> 1), 2, 'b', Vector())))

  //sum
  (graph fold 0) { (ctx, c) ⇒
    ctx.vertex + c
  }
  (graph1 fold 0) { (ctx, c) ⇒
    ctx.vertex + c
  }

  //to count the edges in the graph, we could do:
  (graph fold 0) { (ctx, c) ⇒
    ctx.ins.size + ctx.outs.size + c
  }

  ////to count the nodes in the graph, we could do:
  (graph fold 0) { (ctx, c) ⇒
    1 + c
  }

  (graph fold 0) { (ctx, c) ⇒
    ctx.ins.size + ctx.outs.size + c
  }

  val edges = (graph fold Vector[(String, Int)]())((ctx, acc) ⇒
    ctx.ins ++ ctx.outs ++ acc)

  graph.edges

  graph.reverse

  val relationship = safeMkGraph(
    Seq(LNode("Alice", 34),
      LNode("Bob", 28),
      LNode("Charlie", 32),
      LNode("Sam", 31)),
    Seq(LEdge("Alice", "Bob", "friend"),
      LEdge("Sam", "Bob", "follow"),
      LEdge("Alice", "Charlie", "follow"),
      LEdge("Bob", "Charlie", "follow"),
      LEdge("Alice", "Sam", "follow"))
  )

  relationship dfs Seq("Alice")

  //Alice follow
  (relationship fold Vector[String]()) { (ctx, acc) ⇒
    if (ctx.vertex == "Alice") acc ++ (ctx.inAdj ++ ctx.outAdj).filter {
      _._1 == "follow"
    }.map(_._2)
    else acc
  }

  (relationship fold Vector[String]()) { (ctx, acc) ⇒
    if (ctx.vertex == "Bob") acc ++ (ctx.inAdj).filter {
      _._1 == "follow"
    }.map(_._2)
    else acc
  }

  //Alice friend
  (relationship fold Vector[String]()) { (ctx, acc) ⇒
    if (ctx.vertex == "Alice") acc ++ (ctx.inAdj ++ ctx.outAdj).filter {
      _._1 == "friend"
    }.map(_._2)
    else acc
  }
}
