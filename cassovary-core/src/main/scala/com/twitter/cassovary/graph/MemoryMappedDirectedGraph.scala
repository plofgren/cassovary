/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.graph

import java.util.Date
import java.util.regex.Pattern

import com.twitter.cassovary.util.FastUtilUtils
import com.twitter.cassovary.util.io.{IntLongSource, MemoryMappedIntLongSource}
import java.io._

import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per
 * node: the memory used for n nodes and m edges with both in-neighbor and out-neighbor access is
 * exactly 16 + 16*n + 8*m bytes. Also, loading is very fast because no parsing of text is required.
 * Loading time is exactly the time it takes the operating system to map data from disk into
 * memory.  Nodes are numbered sequentially from 0 to nodeCount - 1 and must be a range of this
 * form (i.e. nodeCount == maxNodeId + 1).
 *
 * When transforming a graph where nodeCount <= maxNodeId
 * to this format, new nodes with no neighbors will be implicitly created.  Currently only supports
 * storing both in-neighbors and out-neighbors of nodes (StoredGraphDir.BothInOut). The binary
 * format is currently subject to change.  Node objects are created on demand when getNodeById is
 * called.
 */

/* Storage format
byteCount  data
8          (reserved, later use for versioning or indicating undirected vs directed)
8          n (i. e. the number of nodes).  Currently must be less than 2^31.
8*(n+1)    Offsets into out-neighbor data. Index i (a Long) points to the out-neighbor data of
           node i.  The out-neighbor data must be stored in sequential order by id, as the
           outegree of node i is computed from the difference in offset between node i+1 and node i.
           Index n is needed to compute the outdegree of node n - 1.
8*(n+1)    Offsets into in-neighbor data (Longs) (Same interpretation as out-neighbor offsets)
m          out-neighbor data
m          in-neighbor data
 */
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph[Node] {
  val data: MemoryMappedIntLongSource = new MemoryMappedIntLongSource(file)

  val nodeCount = data.getLong(8).toInt // In the future we may want to support Long ids, so
  // store nodeCount as Long

  private def outboundOffset(id: Int): Long = data.getLong(16L + 8L * id)

  private def outDegree(id: Int): Int = ((outboundOffset(id + 1) - outboundOffset(id)) / 4).toInt

  private def inboundOffset(id: Int): Long = data.getLong(16L + 8L * (nodeCount + 1) + 8L * id)

  private def inDegree(id: Int): Int = ((inboundOffset(id + 1) - inboundOffset(id)) / 4).toInt

  /* Only created when needed (there is no array of these stored). */
  private class MemoryMappedDirectedNode(override val id: Int) extends Node {
    val nodeOutboundOffset = outboundOffset(id)
    val nodeInboundOffset = inboundOffset(id)
    def outboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = outDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeOutboundOffset + 4L * i)
    }
    def inboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = inDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeInboundOffset + 4L * i)
    }
  }

  def getNodeById(id: Int): Option[Node] =
    if(0 <= id && id < nodeCount) {
      Some(new MemoryMappedDirectedNode(id))
    } else {
      None
    }

  def iterator: Iterator[Node] = (0 to nodeCount).iterator flatMap (i => getNodeById(i))

  lazy val edgeCount: Long = outboundOffset(nodeCount) - outboundOffset(0)

  override lazy val maxNodeId = nodeCount - 1

  val storedGraphDir = StoredGraphDir.BothInOut

  /** Loads the graph data into physical RAM.  Makes a "best effort" (see MappedByteBuffer.load()).
    */
  def loadToRAM(): Unit = {
    data.loadFileToRam()
  }
}

object MemoryMappedDirectedGraph {
  /** Writes the given graph to the given file (overwriting it if it exists) in the current binary
   * format.
   */
  def graphToFile(graph: DirectedGraph[Node], file: File): Unit = {
    val n = graph.maxNodeId + 1 // includes both 0 and maxNodeId as ids
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    out.writeLong(0)
    out.writeLong(n)
    //The outneighbor data starts after the initial 8 bytes, n+1 Longs for outneighbors, and n+1
    // Longs for in-neighbors
    var outboundOffset = 16L + 8L * (n + 1) * 2
    for (i <- 0 until n) {
      out.writeLong(outboundOffset)
      outboundOffset += 4 * (graph.getNodeById(i) map (_.outboundCount)).getOrElse(0)
    }
    out.writeLong(outboundOffset) // Needed to compute outdegree of node n-1

    // The inbound data starts immediately after the outbound data
    var inboundOffset = outboundOffset
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

  /** Parses each line of the given file, assumed to each have the form "<int><whitespace><int>"
    * and calls the given function of each pair of ints.  Empty lines are ignored.
    */
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

  /** Converts a graph to binary format.  The input is a file containing lines  of the form
    * "<id1> <id2>", and this method will throw an IOException if any non-blank line of the file
    * doesn't have this form.
    * The graph in binary format is written to the given file.  This method works by splitting
    * edges into chunks with contiguous id1 (and also chunks with contiguous id2).  Duplicate edges
    * are omitted from the output graph.
    *
    * For performance, the parameter nodesPerChunk can be tuned.  It sets the number of nodes in
    * each temporary file, for example if nodesPerChunk is 10^6, then node ids 0..(10^6-1) will
    * be in the first chunk, ids 10^6..(2*10^6-1) will be in the second chunk, etc.  The amount
    * of RAM used is proportional to the largest total number of edges incident on nodes in any
    * single chunk.
    *
    * If logging function (e.g. System.err.println) is given, progress will be logged there
    * */
  def edgeFileToGraph(edgeListFile: File,
                      graphFile: File,
                      nodesPerChunk: Int = 1000 * 1000,
                      log: String => Unit = (x => Unit)): Unit = {
    val tempFilesById1 = new ArrayBuffer[File]()
    val tempFilesById2 = new ArrayBuffer[File]()
    val outStreamsById1 = new ArrayBuffer[DataOutputStream]()
    val outStreamsById2 = new ArrayBuffer[DataOutputStream]()

    def createAndStoreNewTemporaryFile(fileBuffer: ArrayBuffer[File],
                                       outputStreamBuffer: ArrayBuffer[DataOutputStream]): Unit = {
      val newFile = File.createTempFile("edge_partition", ".bin")
      fileBuffer += newFile
      outputStreamBuffer += new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(newFile)))
      newFile.deleteOnExit() // Request file gets deleted in case an exception happens
    }

    var edgesReadCount = 0L // Only needed for logging
    var maxNodeId = 0

    log("started reading graph at " + new Date())
    forEachEdge(edgeListFile) { (id1, id2) =>
      maxNodeId = math.max(maxNodeId, math.max(id1, id2))
      // Increase the number of temp files if needed
      while (maxNodeId >= tempFilesById1.size * nodesPerChunk) {
        createAndStoreNewTemporaryFile(tempFilesById1, outStreamsById1)
        createAndStoreNewTemporaryFile(tempFilesById2, outStreamsById2)
      }

      val outStreamById1 = outStreamsById1(id1 / nodesPerChunk)
      outStreamById1.writeInt(id1)
      outStreamById1.writeInt(id2)
      val outStreamById2 = outStreamsById2(id2 / nodesPerChunk)
      outStreamById2.writeInt(id1)
      outStreamById2.writeInt(id2)
      edgesReadCount += 1
      if (edgesReadCount % (100 * 1000 * 1000) == 0) {
        log(s"read $edgesReadCount edges")
      }
    }

    outStreamsById1 foreach (_.close())
    outStreamsById2 foreach (_.close())
    log(s"read $edgesReadCount total edges (including any duplicates)")
    log("finished first pass at " + new Date())

    val nodeCount = maxNodeId + 1
    val binaryGraph = new RandomAccessFile(graphFile, "rw")
    binaryGraph.writeLong(0) // Reserved bytes
    binaryGraph.writeLong(nodeCount.toLong)

    // cumulativeNeighborOffset is the byte offset where neighbor data should be written next.
    // The out-neighbor data starts after the initial 16 bytes, n+1 Longs for out-neighbors and n+1
    // Longs for in-neighbors.
    var cumulativeNeighborOffset = 16L + 8L * (nodeCount + 1) * 2

    var halfEdgeCount = 0L // Each edge (id1, id2) has a half-edge for id1 and a half-edge for id2

    // To prevent duplicated code between out-neighbor and in-neighbor writing, iterate over the
    // neighbor types we write (first Out, then In).
    for (neighborType <- List(GraphDir.OutDir, GraphDir.InDir)) {
      val tempFiles = neighborType match {
        case GraphDir.OutDir => tempFilesById1
        case GraphDir.InDir => tempFilesById2
      }
      for ((tempFile, chunkIndex) <- tempFiles.zipWithIndex) {
        // Read edges from temporary file into arrays
        val inStream = new DataInputStream(new BufferedInputStream(
          new FileInputStream(tempFile)))

        // The last chunk might not have the full number of nodes, so compute # nodes in this chunk.
        val nodeCountInChunk = math.min(nodesPerChunk, nodeCount - chunkIndex * nodesPerChunk)
        val neighborArrays = Array.fill(nodeCountInChunk)(new IntArrayList())
        // The difference between a nodeId and the corresponding index into neighborArrays
        val nodeOffset = chunkIndex * nodesPerChunk
        val tempFileEdgeCount = tempFile.length() / 8L
        for (edgeIndex <- 0L until tempFileEdgeCount) {
          val id1 = inStream.readInt()
          val id2 = inStream.readInt()
          neighborType match {
            case GraphDir.OutDir => neighborArrays(id1 - nodeOffset).add(id2)
            case GraphDir.InDir => neighborArrays(id2 - nodeOffset).add(id1)
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
              log(s"wrote $halfEdgeCount half edges")
            cumulativeNeighborOffset += 4
          }
        }
        // Store the ending offset for the last node
        val finalOffset = binaryGraph.getFilePointer

        // Write neighbor offsets to binary file
        val chunkOffsetsStart = neighborType match {
          case GraphDir.OutDir => 16L + 8L * chunkIndex * nodesPerChunk
          case GraphDir.InDir => 16L + 8L * chunkIndex * nodesPerChunk + 8L * (nodeCount + 1)
        }
        binaryGraph.seek(chunkOffsetsStart)

        for (offset <- neighborOffsets) {
          binaryGraph.writeLong(offset)
        }
        // Edge case: For the last chunk, we need to write the final offset, in order to store
        // the nth node's out-degree (and in-degree).
        if (nodesPerChunk * (chunkIndex + 1) >= nodeCount) {
          binaryGraph.writeLong(finalOffset)
        }
      }
    }

    log(s"wrote $halfEdgeCount half-edges")
    binaryGraph.close()
    tempFilesById1 foreach (_.delete())
    tempFilesById2 foreach (_.delete())
    log("finished writing graph at " + new Date())
  }
}
