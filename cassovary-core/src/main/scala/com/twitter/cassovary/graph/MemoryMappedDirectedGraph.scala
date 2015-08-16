package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Path, StandardOpenOption}
import java.nio.channels.FileChannel.MapMode
import java.io.{FileOutputStream, DataOutputStream, BufferedOutputStream, File}

import com.twitter.cassovary.util.io.{MemoryMappedIntLongSource, IntLongSource}

/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per node: the memory
 * used for n nodes and m edges with both in-neighbor and out-neighbor access is exactly 8 + 16*n + 8*m bytes.
 * Also, Loading is very fast because no parsing of text is required.  Loading time is exactly the time it takes the operating system
 * to map data from disk into memory.
 *
 * Currently only supports storing both in-neighbors and out-neighbors of nodes (StoredGraphDir.BothInOut).
 * The binary format is currently subject to change.
 */

/* Storage format
byteCount  data*
4        (reserved, later use for versioning or indicating undirected vs directed)
4        n (i. e. the number of nodes)
8*(n+1)  Offsets into out-neighbor data. Index i (a Long) points to the out-neighbor data of node i. Index n is needed
           to compute the outdegree of node n.
8*(n+1)  Offsets into in-neighbor data (Longs)
m        out-degree data
m        in-degree data
 */
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph[Node] {
  val data: IntLongSource = new MemoryMappedIntLongSource(file)

  override val nodeCount = data.getInt(4)

  private def outboundOffset(id: Int): Long = data.getLong(8L + 8L * id)
  private def outDegree(id: Int): Int = ((outboundOffset(id + 1) - outboundOffset(id)) / 4).toInt
  private def inboundOffset(id: Int): Long = data.getLong(8L + 8L * (nodeCount + 1) + 8L * id)
  private def inDegree(id: Int): Int = ((inboundOffset(id + 1) - inboundOffset(id)) / 4).toInt

  class MemoryMappedDirectedNode(override val id: Int) extends Node {
    val nodeOutboundOffset = outboundOffset(id)
    val nodeInboundOffset = inboundOffset(id)
    override def outboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = outDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeOutboundOffset + 4L * i)
    }
    override def inboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = inDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeInboundOffset + 4L * i)
    }
  }

  override def getNodeById(id: Int): Option[Node] =
    if(id < nodeCount) {
      Some(new MemoryMappedDirectedNode(id))
    } else {
      None
    }

  override def iterator: Iterator[Node] = (0 to nodeCount).iterator flatMap (i => getNodeById(i))

  override def edgeCount: Long = outboundOffset(nodeCount) - outboundOffset(0)

  override val storedGraphDir = StoredGraphDir.BothInOut
}

object MemoryMappedDirectedGraph {
  /** Writes the given graph to the given file (overwriting it if it exists) in the current binary format.
   */
  def graphToFile(graph: DirectedGraph[Node], file: File): Unit = {
    val n = graph.maxNodeId + 1 // includes both 0 and maxNodeId as ids
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    out.writeInt(0)
    out.writeInt(n)
    //The outneighbor data starts after the 8 byte n, n+1 Longs for outneighbors and n+1 Longs for in-neighbors
    var outboundOffset = 8L + 8L * (n + 1) * 2
    for (i <- 0 until n) {
      out.writeLong(outboundOffset)
      println(s"node $i's outneighbor index: $outboundOffset")
      outboundOffset += 4 * (graph.getNodeById(i) map (_.outboundCount)).getOrElse(0)
    }
    out.writeLong(outboundOffset) // Needed to compute outdegree of node n-1

    var inboundOffset = outboundOffset // The inbound data starts immediately after the outbound data
    for (i <- 0 until n) {
      out.writeLong(inboundOffset)
      inboundOffset += 4 * (graph.getNodeById(i) map (_.inboundCount)).getOrElse(0)
    }
    out.writeLong(inboundOffset) // Needed to compute indegree of node n-1

    for (i <- 0 until n) {
      for (v <- (graph.getNodeById(i) map (_.outboundNodes())).getOrElse(Nil)) {
        out.writeInt(v)
      }
    }
    for (i <- 0 until n) {
      for (v <- (graph.getNodeById(i) map (_.inboundNodes())).getOrElse(Nil)) {
        out.writeInt(v)
      }
    }
    out.close()
  }
}
