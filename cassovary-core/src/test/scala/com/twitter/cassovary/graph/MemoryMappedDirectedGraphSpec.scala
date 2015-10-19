package com.twitter.cassovary.graph

import java.io.{IOException, BufferedWriter, FileWriter, File}
import java.nio.file.NoSuchFileException

import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class MemoryMappedDirectedGraphSpec extends WordSpec with Matchers {
  val testGraph1 = ArrayBasedDirectedGraph.apply(
    Iterable(
      NodeIdEdgesMaxId(1, Array(2, 3)),
      NodeIdEdgesMaxId(3, Array(1, 2)),
      NodeIdEdgesMaxId(5, Array(1))
    ),
    StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  def stringToTemporaryFile(contents: String): File = {
    val f = File.createTempFile("invalidGraph1", ".txt")
    val writer = new FileWriter(f)
    writer.write(contents)
    writer.close()
    f.deleteOnExit()
    f
  }

  def graphToEdgeFormat(graph: DirectedGraph[Node],
                        edgeFile: File): Unit = {
    val edges = new ArrayBuffer[(Int, Int)]
    for (u <- testGraph1) {
      for (v <- u.outboundNodes) {
        edges += ((u.id, v))
      }
    }
    edgesToFile(edges, edgeFile)
  }

  def edgesToFile(edges: Seq[(Int, Int)], edgeFile: File): Unit = {
    val writer = new BufferedWriter(new FileWriter(edgeFile))
    for ((u, v) <- edges) {
      writer.write(u + " " + v + "\n")
    }
    // Test that empty lines are accepted by parser
    writer.write("\n")
    writer.close()
  }

  "A MemoryMappedDirectedGraph" should {
    "correctly store and read a graph from binary" in {
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
    }

    "throw an error given an invalid filename" in {
      a[NoSuchFileException] should be thrownBy {
        new MemoryMappedDirectedGraph(new File("nonexistant_file_4398219812437401"))
      }
    }

    " correctly read a list of edges" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val tempEdgeFile = File.createTempFile("graph1", ".txt")
      graphToEdgeFormat(testGraph1, tempEdgeFile)
      MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile, nodesPerChunk = 3)
      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes.toArray should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes.toArray should contain theSameElementsAs (testNode.inboundNodes)
      }
    }

    " correctly read edge lists and remove duplicates" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val edgeString = "1 \t 2\n\n\n1 0\n1 2\n3 \t 1\n1 5\n1 3\n"
      val tempEdgeFile = stringToTemporaryFile(edgeString)
      for (nodesPerChunk <- Seq(3, 5, Integer.MAX_VALUE)) {
        MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile, nodesPerChunk)
        val graph = new MemoryMappedDirectedGraph(tempBinaryFile)
        graph.nodeCount shouldEqual (6)
        val node1 = graph.getNodeById(1).get
        node1.outboundNodes should contain theSameElementsInOrderAs Seq(0, 2, 3, 5)
        node1.inboundNodes should contain theSameElementsInOrderAs Seq(3)
        val node2 = graph.getNodeById(2).get
        node2.inboundNodes should contain theSameElementsInOrderAs Seq(1)
        val node5 = graph.getNodeById(5).get
        node5.inboundNodes should contain theSameElementsInOrderAs Seq(1)
      }
    }

    " throw an error given an invalid edge file" in {
      val outputFile = File.createTempFile("graph", "dat")
      val invalidFile1 = stringToTemporaryFile("1 2\n3")
      an[IOException] should be thrownBy {
        MemoryMappedDirectedGraph.edgeFileToGraph(invalidFile1, outputFile)
      }
      val invalidFile2 = stringToTemporaryFile("1 2\n3 4 5")
      an[IOException] should be thrownBy {
        MemoryMappedDirectedGraph.edgeFileToGraph(invalidFile1, outputFile)
      }
      val validFile1 = stringToTemporaryFile("1 \t\t 2\n\n\n3\t4")
      // Shouldn't throw an exception
      MemoryMappedDirectedGraph.edgeFileToGraph(validFile1, outputFile)
    }

  }
}
