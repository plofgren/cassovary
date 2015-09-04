package com.twitter.cassovary.graph

import java.io._
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

  def graphToEdgeFormat(graph: DirectedGraph[Node],
                        edgeFile: File,
                        sortById2: Boolean): Unit = {
    val edges = new ArrayBuffer[(Int, Int)]
    for (u <- testGraph1) {
      for (v <- u.outboundNodes) {
        edges += ((u.id, v))
      }
    }
    val sortedEdges = if (sortById2) {
      edges.sortBy(_._2)
    } else {
      edges.sortBy(_._1)
    }
    edgesToFile(sortedEdges, edgeFile)
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

  def stringToTemporaryFile(contents: String): File = {
    val f = File.createTempFile("invalidGraph1", ".txt")
    val writer = new FileWriter(f)
    writer.write(contents)
    writer.close()
    f.deleteOnExit()
    f
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
      graphToEdgeFormat(testGraph1, tempEdgeFile, false)
      println(tempEdgeFile.toPath)
      MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile)
      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes should contain theSameElementsAs (testNode.inboundNodes)
      }
    }

    " correctly read a pair of sorted lists of edges" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val tempEdgeFile1 = File.createTempFile("graph1_by_id1", ".txt")
      val tempEdgeFile2 = File.createTempFile("graph1_by_id2", ".txt")
      graphToEdgeFormat(testGraph1, tempEdgeFile1, false)
      graphToEdgeFormat(testGraph1, tempEdgeFile2, true)

      MemoryMappedDirectedGraph.sortedEdgeFilesToGraph(tempEdgeFile1, tempEdgeFile2, tempBinaryFile)
      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes should contain theSameElementsAs (testNode.inboundNodes)
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
