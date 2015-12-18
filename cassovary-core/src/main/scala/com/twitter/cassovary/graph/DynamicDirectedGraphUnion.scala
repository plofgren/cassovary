package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.graph.node.DynamicNode

/**
 * Wraps a directed graph and a dynamic directed graph to create a dynamic graph with the union of their nodes and edges.  When
 * edge are added, they are added to the underlying dynamic graph.  Node or edge deletion is not supported.
 */
class DynamicDirectedGraphUnion(staticGraph: DirectedGraph[Node], dynamicGraph: DynamicDirectedGraph[Node])
    extends DynamicDirectedGraph[Node] {
  // Because computing nodeCount as an intersection is expensive, maintain nodeCount as a variable.
  private var _nodeCount = if (dynamicGraph.nodeCount == 0)
    staticGraph.nodeCount
  else
    ((staticGraph map (_.id)).toSet ++ (dynamicGraph map (_.id)).toSet).size

  override def getNodeById(id: Int): Option[Node] = {
    (staticGraph.getNodeById(id), dynamicGraph.getNodeById(id)) match {
      case (Some(node), None) => Some(node)
      case (None, Some(node)) => Some(node)
      case (Some(leftNode), Some(rightNode)) => Some(new NodeUnion(leftNode, rightNode))
      case (None, None) => None
    }
  }

  /**
   * Adds the given edge to the underlying dynamic graph. Note that for efficiency we don't check if the edge already exists,
   * so if the edge already exists, a 2nd copy of it will be added.
   */
  override def addEdge(srcId: Int, destId: Int): Unit = {
    if (!existsNodeId(srcId)) _nodeCount += 1
    if (!existsNodeId(destId)) _nodeCount += 1
    dynamicGraph.addEdge(srcId, destId)
  }

  /** Not supported. */
  override def removeEdge(srcId: Int, destId: Int): (Option[Node], Option[Node]) = throw new UnsupportedOperationException()

  override def edgeCount: Long = staticGraph.edgeCount + dynamicGraph.edgeCount
  
  override def nodeCount: Int = _nodeCount

  assert(staticGraph.storedGraphDir == dynamicGraph.storedGraphDir)
  override val storedGraphDir: StoredGraphDir = dynamicGraph.storedGraphDir

  override def iterator: Iterator[Node] = {
    val staticGraphIds = staticGraph.iterator map (_.id)
    val additionalDynamicGraphIds = dynamicGraph.iterator map (_.id) filter (!staticGraph.existsNodeId(_))
    (staticGraphIds ++ additionalDynamicGraphIds) map (id => getNodeById(id).get)
  }

  def maxNodeId: Int = math.max(staticGraph.maxNodeId, dynamicGraph.maxNodeId)

  /*  For efficiency, degree and neighbor calls can be overridden to allow graph traversal
      * without creation of node objects.
      */
  override def outDegree(id: Int): Int = staticGraph.outDegree(id) + dynamicGraph.outDegree(id)

  override def inNeighborId(id: Int, i: Int): Int =
    if (i < staticGraph.inDegree(id))
      staticGraph.inNeighborId(id, i)
    else
      dynamicGraph.inNeighborId(id, i - staticGraph.inDegree(id))

  override def inDegree(id: Int): Int = staticGraph.inDegree(id) + dynamicGraph.inDegree(id)

  /** Returns the ith out-neighbor of the node with the given id.
    * TODO: Specify exceptions
    * */
  override def outNeighborId(id: Int, i: Int): Int =
    if (i < staticGraph.outDegree(id))
      staticGraph.outNeighborId(id, i)
    else
      dynamicGraph.outNeighborId(id, i - staticGraph.outDegree(id))

}

/** Represents the union of two nodes. */
private class NodeUnion(leftNode: Node,
                        rightNode: Node) extends Node {
  override val id: Int = rightNode.id

  override def inboundNodes(): Seq[Int] =
    new IndexedSeqUnion(leftNode.inboundNodes(), rightNode.inboundNodes())

  override def outboundNodes(): Seq[Int] =
    new IndexedSeqUnion(leftNode.outboundNodes(), rightNode.outboundNodes())

}

/** Represents the concatenation of two IndexedSeqs. */
// TODO: We assume xs and ys have efficient random access (are effectively IndexedSeqs).  Refactoring Node to return
// IndexedSeq would remove this assumption
private class IndexedSeqUnion[A](xs: Seq[A], ys: Seq[A]) extends IndexedSeq[A] {
  override def length: Int = xs.size + ys.size

  override def apply(i: Int): A =
    if (i < xs.size) {
      xs(i)
    } else if (i - xs.size < ys.size) {
      ys(i - xs.size)
    } else {
      throw new IndexOutOfBoundsException(s"Invalid index $i")
    }
}
