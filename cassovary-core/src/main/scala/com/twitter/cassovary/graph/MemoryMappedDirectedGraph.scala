package com.twitter.cassovary.graph

import java.io._
import java.util.Date
import java.util.regex.Pattern

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.io.{AdjacencyListGraphReader, MemoryMappedIntLongSource}
import com.twitter.cassovary.util.{FastUtilUtils, NodeNumberer}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}

import scala.io.Source

/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per node: the memory
 * used for n nodes and m edges with both in-neighbor and out-neighbor access is exactly 8 + 16*n + 8*m bytes.
 * Also, Loading is very fast because no parsing of text is required.  Loading time is exactly the time it takes the operating system
 * to map data from disk into memory.
 *
 * Nodes are numbered sequentially from 0 to nodeCount - 1 and must be a range of this form (i.e. nodeCount == maxNodeId + 1).
 * When transforming a graph where nodeCount <= maxNodeId to this format, new nodes with no neighbors will be implicitly created.
 *
 * Currently only supports storing both in-neighbors and out-neighbors of nodes (StoredGraphDir.BothInOut).
 * The binary format is currently subject to change.
 */

/* Storage format
byteCount  data
4          (reserved, later use for versioning or indicating undirected vs directed)
4          n (i. e. the number of nodes)
8*(n+1)    Offsets into out-neighbor data. Index i (a Long) points to the out-neighbor data of node i.
           Index n is needed to compute the outdegree of node n - 1.
8*(n+1)    Offsets into in-neighbor data (Longs) (Same interpretation as out-neighbor offsets)
m          out-neighbor data
m          in-neighbor data
 */
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph[Node] {
  val data: MemoryMappedIntLongSource = new MemoryMappedIntLongSource(file)

  override val nodeCount = data.getInt(4)

  private def outboundOffset(id: Int): Long = data.getLong(8L + 8L * id)

  private def outDegree(id: Int): Int = ((outboundOffset(id + 1) - outboundOffset(id)) / 4).toInt

  private def inboundOffset(id: Int): Long = data.getLong(8L + 8L * (nodeCount + 1) + 8L * id)

  private def inDegree(id: Int): Int = ((inboundOffset(id + 1) - inboundOffset(id)) / 4).toInt

  /* Only created when needed (there is no array of these stored). */
  private class MemoryMappedDirectedNode(override val id: Int) extends Node {
    val nodeOutboundOffset = outboundOffset(id)
    val nodeInboundOffset = inboundOffset(id)
    override def outboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = outDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeOutboundOffset + 4L * i)
      override def toString(): String = "Memory Mapped IndexedSeq " + mkString("[", ",", "]")
    }
    override def inboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = inDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeInboundOffset + 4L * i)
      override def toString(): String = "Memory Mapped IndexedSeq " + mkString("[", ",", "]")
    }
  }

  override def getNodeById(id: Int): Option[Node] =
    if(0 <= id && id < nodeCount) {
      Some(new MemoryMappedDirectedNode(id))
    } else {
      None
    }

  override def iterator: Iterator[Node] = (0 to nodeCount).iterator flatMap (i => getNodeById(i))

  override def edgeCount: Long = (outboundOffset(nodeCount) - outboundOffset(0)) / 4

  override lazy val maxNodeId: Int = nodeCount - 1
  // Uncomment in future if maxNodeId becomes a method:
  // override def maxNodeId = nodeCount - 1

  override val storedGraphDir = StoredGraphDir.BothInOut

  def loadGraphToRam(): Unit = data.loadFileToRam()
}

object MemoryMappedDirectedGraph {
  /** Writes the given graph to the given file (overwriting it if it exists) in the current binary format.
    */
  def graphToFile(graph: DirectedGraph[Node], file: File): Unit = {
    val n = graph.maxNodeId + 1 // includes both 0 and maxNodeId as ids
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))

    def outDegree(id: Int): Int = (graph.getNodeById(id) map (_.outboundCount)).getOrElse(0)
    def inDegree(id: Int): Int = (graph.getNodeById(id) map (_.inboundCount)).getOrElse(0)
    writeHeaderAndDegrees(n, outDegree, inDegree, out)

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

  private def forEachEdge(edgeListFile: File)(f: (Int, Int) => Unit): Unit = {
    val linePattern = Pattern.compile(raw"(\d+)\s+(\d+)")
    for (line <- Source.fromFile(edgeListFile).getLines()
         if line.nonEmpty) {
      val matcher = linePattern.matcher(line)
      if (!matcher.matches()) {
        throw new IOException("invalid line in edge file: " + line)
      } else {
        val u = matcher.group(1).toInt // Groups are 1-indexed
        val v = matcher.group(2).toInt
        f(u, v)
      }
    }
  }

  /**
   * Scans through the given file of whitespace separated(srcId, destId) pairs and returns two arrays (outDegrees, inDegrees)
   * of length 1 greater than the maximum node id seen.
   */
  private def accumulateOutAndInDegrees(edgeListFile: File): (IntList, IntList) = {
    val outDegrees = new IntArrayList()
    val inDegrees = new IntArrayList()
    forEachEdge(edgeListFile) { (u, v) =>
      val maxNodeId = math.max(u, v)
      while (maxNodeId >= outDegrees.size()) {
        outDegrees.add(0)
        inDegrees.add(0)
      }
      outDegrees.set(u, outDegrees.get(u) + 1)
      inDegrees.set(v, inDegrees.get(v) + 1)
    }
    (outDegrees, inDegrees)
  }

  /** Writes the graph header and outdegree information to the given DataOutput, leaving the pointer of the DataOutput
    * at the end of the degree information
    */
  private def writeHeaderAndDegrees(nodeCount: Int, outDegree: Int => Int, inDegree: Int => Int, out: DataOutput): Unit = {
    out.writeInt(0)
    out.writeInt(nodeCount)
    // The out-neighbor data starts after the initial 8 bytes, n+1 Longs for out-neighbors and n+1 Longs for in-neighbors
    var outboundOffset = 8L + 8L * (nodeCount + 1) * 2
    for (i <- 0 until nodeCount) {
      out.writeLong(outboundOffset)
      outboundOffset += 4 * outDegree(i)
    }
    out.writeLong(outboundOffset) // Needed to compute outdegree of node n-1

    // The inbound data starts immediately after the outbound data
    var inboundOffset = outboundOffset
    for (i <- 0 until nodeCount) {
      out.writeLong(inboundOffset)
      inboundOffset += 4 * inDegree(i)
    }
    out.writeLong(inboundOffset) // Needed to compute indegree of node n-1
  }

  /** Converts a graph to binary format.  The input is a file containing lines  of the form
    * "<id1> <id2>", and this method will throw an IOException if any non-blank line of the file doesn't have this form.
    * The graph in binary format is written to the given file.
    * TODO: This method is currently rather slow (~5 hours on a ~800M edge graph), and would likely
    * be faster if we used a memory mapped file rather than a RandomAccessFile.  The method
    * sortedEdgeFilesToGraph is currently faster (~1 hour on a ~1B edge graph).
    * */
  def edgeFileToGraphSlow(edgeListFile: File, graphFile: File): Unit = {
    val (outDegrees, inDegrees) = accumulateOutAndInDegrees(edgeListFile)
    System.err.println("finished reading degrees at " + new Date())
    val nodeCount = outDegrees.size()
    val out = new RandomAccessFile(graphFile, "rw")
    def outDegree(id: Int): Int = outDegrees.get(id)
    def inDegree(id: Int): Int = inDegrees.get(id)
    writeHeaderAndDegrees(nodeCount, outDegree, inDegree, out)

    // outboundOffsets(i) stores the offset where the next out-neighbor of node i should be written
    val outboundOffsets = new Array[Long](nodeCount)
    //The outneighbor data starts after the initial 8 bytes, n+1 Longs for outneighbors and n+1 Longs for in-neighbors
    var cumulativeOffset = 8L + 8L * (nodeCount + 1) * 2
    for (i <- 0 until nodeCount) {
      outboundOffsets(i) = cumulativeOffset
      cumulativeOffset += 4 * outDegree(i)
    }

    // inboundOffsets(i) stores the offset where the next out-neighbor of node i should be written
    val inboundOffsets = new Array[Long](nodeCount)
    for (i <- 0 until nodeCount) {
      inboundOffsets(i) = cumulativeOffset
      cumulativeOffset += 4 * inDegree(i)
    }
    
    var edgeCount = 0L
    // Write each edge at the correct location
    forEachEdge(edgeListFile) { (u, v) =>
      out.seek(outboundOffsets(u))
      out.writeInt(v)
      outboundOffsets(u) += 4
      out.seek(inboundOffsets(v))
      out.writeInt(u)
      inboundOffsets(v) += 4
      if (edgeCount % (100*1000*1000) == 0)
        System.err.println(s"wrote $edgeCount edges")
      edgeCount += 1
    }
    System.err.println(s"wrote $edgeCount edges")
    out.close()
  }

  /**
   * Scans through the given file of whitespace separated(srcId, destId) pairs and returns two arrays (outDegrees, inDegrees)
   * of length 1 greater than the maximum node id seen.
   */
  private def accumulateOutAndInDegreesAndForeachEdge(edgeListFile: File)
                                                     (processEdge: (Int, Int) => Unit):
      (IntList, IntList) = {
    val outDegrees = new IntArrayList()
    val inDegrees = new IntArrayList()
    forEachEdge(edgeListFile) { (u, v) =>
      val maxNodeId = math.max(u, v)
      while (maxNodeId >= outDegrees.size()) {
        outDegrees.add(0)
        inDegrees.add(0)
      }
      outDegrees.set(u, outDegrees.get(u) + 1)
      inDegrees.set(v, inDegrees.get(v) + 1)
    }
    (outDegrees, inDegrees)
  }

  /** Converts a graph to binary format.  The input is a file containing lines  of the form
    * "<id1> <id2>", and this method will throw an IOException if any non-blank line of the file doesn't have this form.
    * The graph in binary format is written to the given file.  This method works by splitting
    * edges into chunks with contiguous id1 (and also chunks with contiguous id2).  Duplicate edges
    * are omitted from the output graph.
    *
    * For efficiency, a maxNodeIdBound (which must be at least as large as the maximum id in
    * the graph) and chunkCount should be specified to aid in balancing temporary files.  For
    * example, if maxNodeIdBound is 10^9 and chunkCount is 100, then node ids 0..(10^7-1) will be in the
    * first chunk, ids 10^7..(2*10^7-1) will be in the second chunk, etc.  The amount of RAM used
    * is proportional to the largest total number of edges incident on nodes in any single chunk.
    *
    * TODO (comment on speed)
    * */
  def edgeFileToGraph(edgeListFile: File,
                      graphFile: File,
                      maxNodeIdBound: Int = Integer.MAX_VALUE,
                      chunkCount: Int = 1000): Unit = {
    val tempFilesById1: IndexedSeq[File] = (0 until chunkCount) map { i =>
      File.createTempFile(s"edges_by_id_1_$i", ".bin")
    }
    val outStreamsById1 = tempFilesById1 map {
      f => new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))
    }
    val tempFilesById2: IndexedSeq[File] = (0 until chunkCount) map { i =>
      File.createTempFile(s"edges_by_id_2_$i", ".bin")
    }
    val outStreamsById2 = tempFilesById2 map {
      f => new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))
    }
    // Request files get deleted in case an exception happens
    tempFilesById1 foreach (_.deleteOnExit())
    tempFilesById2 foreach (_.deleteOnExit())

    // Add 1 so maxIdBound < nodesPerChunk * temporaryFileCount
    val nodesPerChunk = maxNodeIdBound / chunkCount + 1

    var edgesReadCount = 0
    var maxNodeId = 0 // We need the exact maxNodeId for reserving neighbor offset areas
    System.err.println("started reading graph at " + new Date())
    forEachEdge(edgeListFile) { (id1, id2) =>
      val outStreamById1 = outStreamsById1(id1 / nodesPerChunk)
      outStreamById1.writeInt(id1)
      outStreamById1.writeInt(id2)
      val outStreamById2 = outStreamsById2(id2 / nodesPerChunk)
      outStreamById2.writeInt(id1)
      outStreamById2.writeInt(id2)
      maxNodeId = math.max(maxNodeId, math.max(id1, id2))
      edgesReadCount += 1
      if (edgesReadCount % (100 * 1000 * 1000) == 0) {
        System.err.println(s"read $edgesReadCount edges")
      }
    }

    outStreamsById1 foreach (_.close())
    outStreamsById2 foreach (_.close())
    System.err.println(s"read $edgesReadCount total edges (including any duplicates)")
    System.err.println("finished first pass at " + new Date())

    if (maxNodeId > maxNodeIdBound) {
      throw new IllegalArgumentException(
        s"given maxNodeIdBound $maxNodeIdBound yet maxNodeId is $maxNodeId")
    }

    val nodeCount = maxNodeId + 1
    val nonemptyChunkCount = (nodeCount + nodesPerChunk - 1) / nodesPerChunk
    val binaryGraph = new RandomAccessFile(graphFile, "rw")
    binaryGraph.writeInt(0) // Reserved bytes TODO: change to long before merging to master
    binaryGraph.writeInt(nodeCount) // TODO: change to long before merging to master

    // cumulativeNeighborOffset is the byte offset where neighbor data should be written next
    //The outneighbor data starts after the initial 8 bytes, n+1 Longs for out-neighbors and n+1
    // Longs for in-neighbors
    var cumulativeNeighborOffset = 8L + 8L * (nodeCount + 1) * 2

    // To prevent duplicated code between out-neighbor and in-neighbor writing, we'll use an enum
    // to keep track of which we are writing (first Out, then In)
    object NeighborType extends Enumeration {
      val Out, In = Value
    }

    var halfEdgeCount = 0L // Each edge (id1, id2) has a half-edge for id1 and a half-edge for id2
    for (neighborType <- List(NeighborType.Out, NeighborType.In)) {
      val tempFiles = neighborType match {
        case NeighborType.Out => tempFilesById1
        case NeighborType.In => tempFilesById2
      }
      val relevantTempFiles = tempFiles take nonemptyChunkCount
      for ((tempFile, chunkIndex) <- relevantTempFiles.zipWithIndex) {
        val nodeOffset = chunkIndex * nodesPerChunk
        println(s"nodeOffset: $nodeOffset tmpFile: ${tempFile.getCanonicalPath}")
        // Read edges from temporary file into arrays
        val inStream = new DataInputStream(new BufferedInputStream(
          new FileInputStream(tempFile)))

        // The last chunk might not have the full number of nodes, so compute # nodes in this chunk.
        val nodeCountInChunk = math.min(nodesPerChunk, nodeCount - chunkIndex * nodesPerChunk)
        val neighborArrays = Array.fill(nodeCountInChunk)(new IntArrayList())
        val tempFileEdgeCount = tempFile.length() / 8L
        for (edgeIndex <- 0L until tempFileEdgeCount) {
          val id1 = inStream.readInt()
          val id2 = inStream.readInt()
          neighborType match {
            case NeighborType.Out => neighborArrays(id1 - nodeOffset).add(id2)
            case NeighborType.In => neighborArrays(id2 - nodeOffset).add(id1)
          }
        }
        inStream.close()

        // Write edge data to binary file and store neighbor offsets
        val neighborOffsets = new Array[Long](nodeCountInChunk)
        binaryGraph.seek(cumulativeNeighborOffset)
        for ((neighbors, i) <- neighborArrays.zipWithIndex) {
          neighborOffsets(i) = binaryGraph.getFilePointer
          val sortedDistinctNeighbors = FastUtilUtils.sortedDistinctInts(neighbors)
          for (j <- 0 until sortedDistinctNeighbors.size()) {
            val neighborId = sortedDistinctNeighbors.get(j)
            binaryGraph.writeInt(neighborId)
            halfEdgeCount += 1
            if (halfEdgeCount % (100 * 1000 * 1000) == 0)
              System.err.println(s"wrote $halfEdgeCount half edges")
            cumulativeNeighborOffset += 4
          }
        }
        // Store the ending offset for the last node
        val finalOffset = binaryGraph.getFilePointer

        // Write neighbor offsets to binary file
        val chunkOffsetsStart = neighborType match {
          case NeighborType.Out => 8L + 8L * chunkIndex * nodesPerChunk
          case NeighborType.In => 8L + 8L * chunkIndex * nodesPerChunk + 8L * (nodeCount + 1)
        }
        binaryGraph.seek(chunkOffsetsStart)

        for (offset <- neighborOffsets) {
          binaryGraph.writeLong(offset)
        }
        // Edge case: For the last chunk, we need to write the final offset, in order to know
        // the nth node's out-degree (and in-degree).
        if (nodesPerChunk * (chunkIndex + 1) >= nodeCount) {
          binaryGraph.writeLong(finalOffset)
        }
      }
    }

    System.err.println(s"wrote $halfEdgeCount half-edges")
    binaryGraph.close()
    tempFilesById1 foreach (_.delete())
    tempFilesById2 foreach (_.delete())
  }

  /** Converts a graph to binary format.  The input is a pair of files containing edges stored
    * as lines of the form "<id1> <id2>", and this method will throw an IOException if any non-blank line of either file doesn't have this form.
    * The input files must have an identical set of edges, and the first file must be sorted by id1, and the second by id2.  The
    * graph in binary format is written to the given file.  */
  def sortedEdgeFilesToGraph(edgeListFileSortedById1: File,
                             edgeListFileSortedById2: File,
                             graphFile: File): Unit = {
    val (outDegrees, inDegrees) = accumulateOutAndInDegrees(edgeListFileSortedById1)
    System.err.println("finished reading degrees at " + new Date())
    val nodeCount = outDegrees.size()
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(graphFile)))
    def outDegree(id: Int): Int = outDegrees.get(id)
    def inDegree(id: Int): Int = inDegrees.get(id)
    writeHeaderAndDegrees(nodeCount, outDegree, inDegree, out)

    var edgeCount = 0L
    // Write out-neighbors.  Note that they are already sorted by id1, so we just need to write them directly to the
    // edge data in the output
    forEachEdge(edgeListFileSortedById1) { (u, v) =>
      out.writeInt(v)
      if (edgeCount % (100*1000*1000) == 0)
        System.err.println(s"wrote $edgeCount half-edges")
      edgeCount += 1
    }
    // Write in-neighbors.
    forEachEdge(edgeListFileSortedById2) { (u, v) =>
      out.writeInt(u)
      if (edgeCount % (100*1000*1000) == 0)
        System.err.println(s"wrote $edgeCount half-edges")
      edgeCount += 1
    }
    System.err.println(s"wrote $edgeCount half-edges.  Finished at " + new Date())
    out.close()
  }
}


// TODO: This is temporary; remove it and possibly replace it with a more robust command line
// tool for converting graphs to binary format.
object MemoryMappedDirectedGraphBenchmark {
  def readAdjacencyListToGraph(graphPath: String): DirectedGraph[Node] = {
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
    val testNodeId = 1
    val graphName = args(1)
    if (args(0) == "readAdj") {
      val graph = readAdjacencyListToGraph(graphName)
      println(s"outneighbors of node $testNodeId: " + graph.getNodeById(testNodeId).get.outboundNodes())
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to read adj graph: $loadTime")

      val binaryFileName = graphName.substring(0, graphName.lastIndexOf(".")) + ".dat"
      startTime = System.currentTimeMillis()
      MemoryMappedDirectedGraph.graphToFile(graph, new File( binaryFileName))
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to write binary graph: $writeTime")
    } else if (args(0) == "readBin") {
      val graph = new MemoryMappedDirectedGraph(new File(graphName))
      println(s"outneighbors of node $testNodeId: " + graph.getNodeById(testNodeId).get.outboundNodes())
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to read binary graph: $loadTime")
    } else if (args(0) == "readEdgesSimple") {
      val binaryFileName = graphName.substring(0, graphName.lastIndexOf(".")) + ".dat"
      MemoryMappedDirectedGraph.edgeFileToGraph(new File(graphName), new File(binaryFileName),
        Integer.MAX_VALUE, 1)
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to convert graph: $writeTime")
    } else if (args(0) == "readEdges") {
      assert(args.length == 4)
      val binaryFileName = graphName.substring(0, graphName.lastIndexOf(".")) + ".dat"
      val maxNodeIdBound = args(2).toInt
      val chunkCount = args(3).toInt
      MemoryMappedDirectedGraph.edgeFileToGraph(new File(graphName), new File(binaryFileName),
        maxNodeIdBound, chunkCount)
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Time to convert graph: $writeTime")
    } else if (args(0) == "readEdgesSorted") {
      assert(args.length == 4, "arguments: readEdgesSorted <edge file sorted by id1> " +
        "<edge file sorted by id2> <binary output file>")
      val binaryFileName = args(3)
      MemoryMappedDirectedGraph.sortedEdgeFilesToGraph(
        new File(args(1)),
        new File(args(2)),
        new File(binaryFileName))
      val writeTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Seconds to convert graph: $writeTime")
    } else {
      println("unexpected command")
    }
  }
}
