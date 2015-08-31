package com.twitter.cassovary.graph

import java.io.{BufferedWriter, PrintWriter, FileWriter, File}
import java.nio.file.NoSuchFileException

import org.scalatest.{Matchers, WordSpec}

class MemoryMappedDirectedGraphSpec extends WordSpec with Matchers {
  val testGraph1 = ArrayBasedDirectedGraph.apply(
    Iterable(
      NodeIdEdgesMaxId(1, Array(2, 3)),
      NodeIdEdgesMaxId(3, Array(1, 2)),
      NodeIdEdgesMaxId(5, Array(1))
    ),
    StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  def graphToEdgeFormat(graph: DirectedGraph[Node], edgeFile: File): Unit = {
    val writer = new BufferedWriter(new FileWriter(edgeFile))
    for (u <- testGraph1) {
      for (v <- u.outboundNodes) {
        writer.write(u.id + " " + v + "\n")
      }
    }
    writer.close()
  }
  "A MemoryMappedDirectedGraph" should {
    " correctly store and read a graph from binary" in {
      val tempFile = File.createTempFile("graph1", ".bin")
      MemoryMappedDirectedGraph.graphToFile(testGraph1, tempFile)
      val graph1 = new MemoryMappedDirectedGraph(tempFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes should contain theSameElementsAs (testNode.inboundNodes)
      }
      graph1.getNodeById(-1) should be(None)
      graph1.getNodeById(6) should be(None)
      graph1.getNodeById(1 << 29) should be(None)
      tempFile.deleteOnExit()
    }

    " throw an error given an invalid filename" in {
      a[NoSuchFileException] should be thrownBy {
        new MemoryMappedDirectedGraph(new File("nonexistant_file_4398219812437401"))
      }
    }

    " correctly read a list of edges" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val tempEdgeFile = File.createTempFile("graph1", ".txt")
      graphToEdgeFormat(testGraph1, tempEdgeFile)
      println(tempEdgeFile.toPath)
      MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile)
      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes should contain theSameElementsAs (testNode.inboundNodes)
      }
    }
  }
}
