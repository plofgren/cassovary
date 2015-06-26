package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Path, StandardOpenOption}
import java.nio.channels.FileChannel.MapMode
import java.io.File

/*
 * A graph which reads edge data from a memory mapped file.  Loading is very fast because no
 * parsing of text is required.  Loading time is exactly the time it takes the operating system
 * to map data from disk into memory.  Also, there is no object overhead per node: the memory
 * used for n nodes and m edges is exactly O(1) + 8*n + 4*m, or twice that if both edge
 * directions are stored.
 */

class MemoryMappedDirectedGraph(fileName: String) extends DirectedGraph[Node] {


  /**
   * Returns the node with the given {@code id} or else {@code None} if the given node does not
   * exist in this graph.
   */
  override def getNodeById(id: Int): Option[Node] = ???

  override def iterator: Iterator[Node] = ???

  /**
   * Returns the total number of directed edges in the graph.  A mutual edge, eg: A -> B and B -> A,
   * counts as 2 edges in this total.
   */
  override def edgeCount: Long = ???

  override val storedGraphDir = StoredGraphDir.BothInOut

  /**
   * Returns the number of nodes in the graph.
   */
  override def nodeCount: Int = ???
}

trait IntLongSource {
  def getInt(index: Long): Int
  def getLong(index: Long): Int
}

class MemoryMappedIntLongSource(filename: String) extends IntLongSource {
  val file = new File(filename)
  val fileChannel = FileChannel.open( file.toPath, StandardOpenOption.READ)
  //TODO: multiple mapped files
  // Each buffer uses int addressing, so can only access 2GB.  We'll use multiple buffers,
  // and access 2^30 bytes per buffer
  val bytesPerBuffer = 1L << 30
  // ceiling of file.length() / bytesPerBuffer
  val bufferCount = ((file.length() + bytesPerBuffer - 1) / bytesPerBuffer).toInt
  val byteBuffers = (0 until bufferCount) map { bufferIndex =>
    val size = if (bufferIndex + 1 < bufferCount)
      bytesPerBuffer
    else
      file.length - (bufferCount - 1L) * bytesPerBuffer
    fileChannel.map(MapMode.READ_ONLY, bufferIndex * bytesPerBuffer, size)
  }

  mappedFile.load()

  def getInt(index: Long): Int = mappedFile.getInt(index)
  def getLong(index: Long): Int
}
