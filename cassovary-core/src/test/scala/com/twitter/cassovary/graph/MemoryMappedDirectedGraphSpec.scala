package com.twitter.cassovary.graph

import java.io.File

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node.DynamicNode
import org.scalatest.{Matchers, WordSpec}

class MemoryMappedDirectedGraphSpec extends WordSpec with Matchers {
  val testGraph1 = ArrayBasedDirectedGraph.apply(
    Iterable(
      NodeIdEdgesMaxId(1, Array(2, 3)),
      NodeIdEdgesMaxId(2, Array()),
      NodeIdEdgesMaxId(3, Array(1, 2)),
      NodeIdEdgesMaxId(4, Array()),
      NodeIdEdgesMaxId(5, Array(1))
    ),
    StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  "A MemoryMappedDirectedGraph" should {
    " correctly store and read a graph" in {
      val tempFile = File.createTempFile("graph1", ".bin")
      MemoryMappedDirectedGraph.graphToFile(testGraph1, tempFile)
      val graph1 = new MemoryMappedDirectedGraph(tempFile)
      for (testNode <- testGraph1) {
        val node = graph1.getNodeById(testNode.id).get
        node.outboundNodes should contain theSameElementsAs (testNode.outboundNodes)
        node.inboundNodes should contain theSameElementsAs (testNode.inboundNodes)
      }
    }
  }
}
