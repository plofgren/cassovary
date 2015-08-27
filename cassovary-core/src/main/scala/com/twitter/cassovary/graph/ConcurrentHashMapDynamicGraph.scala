/*
 * Copyright 2015 TODO
 *
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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node.DynamicNode

/**
 * An efficient dynamic graph implementation which supports concurrent reading and writing.  Locks are only used by writing
 * threads, which improves efficiency.
 * Nodes are stored in a ConcurrentHashMap, and neighbors of each node are stored in Array[Int]s.  Currently, only edge
 * and node addition (not deletion) is supported.
 */
class ConcurrentHashMapDynamicGraph(storedGraphDir: StoredGraphDir = BothInOut)
    extends DynamicDirectedGraphHashMap(storedGraphDir) {
  override def nodeFactory(id: Int): DynamicNode = new ConcurrentNode(id)
  override def removeEdge(srcId: Int, destId: Int) = throw new UnsupportedOperationException()
}

private class ConcurrentNode(val id: Int) extends DynamicNode {
  val outboundList = new ConcurrentIntArrayList()
  val inboundList = new ConcurrentIntArrayList()

  override def outboundNodes(): Seq[Int] = outboundList.toSeq
  override def inboundNodes(): Seq[Int] = inboundList.toSeq
  /**
   * Add outbound edges {@code nodeIds} into the outbound list.
   */
  override def addOutBoundNodes(nodeIds: Seq[Int]): Unit =
    outboundList.append(nodeIds)

  /**
   * Add inbound edges {@code nodeIds} into the inbound list.
   */
  override def addInBoundNodes(nodeIds: Seq[Int]): Unit =
    inboundList.append(nodeIds)

  override def removeInBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
  override def removeOutBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
}

/** A resizable array of Ints which supports appending Ints (but not changing or removing current Ints).
  * It supports concurrent reading and writing.
  */
// We store Ints in an array padded with extra capacity that will grow over time
// We essentially want a fastutil IntArrayList, but with synchronization.
private class ConcurrentIntArrayList {
  @volatile private var intArray: Array[Int] = new Array[Int](ConcurrentIntArrayList.initialCapacity)
  @volatile private var size = 0

  def append(ints: Seq[Int]): Unit = {
    this.synchronized {
      if (size + ints.size > intArray.length) {
        val newCapacity = math.max(
          (intArray.length * ConcurrentIntArrayList.resizeFactor).toInt,
          size + ints.size)
        val newIntArray = new Array[Int](newCapacity)
        System.arraycopy(intArray, 0, newIntArray, 0, size)
        intArray = newIntArray
      }
      // Update outgoingArray before updating size, so concurrent reader threads don't read past the end of the array
      for (i <- 0 until ints.size) {
        intArray(i + size) = ints(i)
      }
      size = size + ints.size
    }
  }

  /** Returns an immutable view of the current Ints in this object.
   */
  // Because of volatile references, no lock is needed.
  def toSeq: IndexedSeq[Int] = {
    // First copy the size, since another thread might increase the size if we copy the intArray reference first, causing
    // size to be longer than intArray.
    val result = new IntArrayView(size)
    result.intArray = intArray
    result
  }
}

object ConcurrentIntArrayList {
  val initialCapacity = 2
  val resizeFactor = 2.0
}

/**
 * Stores a reference to an array and a size to create a view of the prefix of the array.  In our use case, the size needs
 * to be set before the intArray to prevent a race condition.
 */
class IntArrayView(override val size: Int) extends IndexedSeq[Int] {
  /** The array this view wraps. */
  var intArray: Array[Int] = _
  override def length: Int = size
  override def apply(idx: Int): Int = intArray(idx)
}
