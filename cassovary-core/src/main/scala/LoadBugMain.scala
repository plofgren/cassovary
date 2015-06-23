import com.twitter.cassovary.graph.{Node, DirectedGraph, StoredGraphDir}
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.NodeNumberer
import com.twitter.cassovary.util.io.AdjacencyListGraphReader

object LoadBugMain {
  def main(args: Array[String]): Unit = {
    val reader = new AdjacencyListGraphReader(
      "cassovary-core/src/test/resources/graphs",
      "test_graph.txt",
      new NodeNumberer.IntIdentity(),
      _.toInt) {
      override def storedGraphDir: StoredGraphDir = StoredGraphDir.BothInOut
    }
    val graph = reader.toSharedArrayBasedDirectedGraph()
    println("Done loading graph. ")
  }
}
