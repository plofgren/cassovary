import java.io._

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.{MemoryMappedDirectedGraph, StoredGraphDir, Node, DirectedGraph}
import com.twitter.cassovary.util.NodeNumberer
import com.twitter.cassovary.util.io.AdjacencyListGraphReader

/**
 * Demonstrates conversion of a graph from adjacency list format to MemoryMappedDirectedGraph
 * binary format.
 */
object MemoryMappedDirectedGraphExample {
  def readGraph(graphPath: String): DirectedGraph[Node] = {
    val filenameStart = graphPath.lastIndexOf('/') + 1
    val graphDirectory = graphPath.take(filenameStart)
    val graphFilename = graphPath.drop(filenameStart)
    println("loading graph:" + graphFilename)

    val reader = new AdjacencyListGraphReader(
      graphDirectory,
      graphFilename,
      new NodeNumberer.IntIdentity(),
      _.toInt) {
      override def storedGraphDir: StoredGraphDir = StoredGraphDir.BothInOut
    }
    reader.toArrayBasedDirectedGraph()
  }

  def main(args: Array[String]): Unit = {
    var startTime = System.currentTimeMillis()
    val testNodeId = 30000000
    lazy val graphName = args(1)
    if (args(0) == "readEdges") {
      assert(args.length >= 2)
      val binaryFileName = graphName.substring(0, graphName.lastIndexOf(".")) + ".dat"
      val nodesPerChunk = if (args.length > 2)
        args(2).toInt
      else
        1000 * 1000
      MemoryMappedDirectedGraph.edgeFileToGraph(new File(graphName), new File(binaryFileName),
        nodesPerChunk, System.err.println)
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to convert graph: $writeTime")
    }
    else if (args(0) == "readAdj") {
      val graph = readGraph(graphName)
      println(s"outneighbors of node $testNodeId: " +
        graph.getNodeById(testNodeId).get.outboundNodes())
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to read adj graph: $loadTime")

      val binaryFileName = graphName.substring(0, graphName.lastIndexOf(".")) + ".dat"
      startTime = System.currentTimeMillis()
      MemoryMappedDirectedGraph.graphToFile(graph, new File( binaryFileName))
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to write binary graph: $writeTime")
    } else if (args(0) == "readBin") {
      val graph = new MemoryMappedDirectedGraph(new File(graphName))
      println(s"outneighbors of node $testNodeId: " +
        graph.getNodeById(testNodeId).get.outboundNodes())
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to read binary graph: $loadTime")
    } else if (args(0) == "speed_raf") {
      val f = new RandomAccessFile("out_raf.bin", "rw")
      for (i <- 0 until 25 * 1000 * 1000) {
        f.writeInt(i)
      }
      f.close()
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to write random access file: $writeTime")
    } else if (args(0) == "speed_buf") {
      val f = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("out_buf.bin")))
      for (i <- 0 until 25 * 1000 * 1000) {
        f.writeInt(i)
      }
      f.close()
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to write buffered file: $writeTime")
    }
  }
}
