/*
 * Copyright 2014 Twitter, Inc.
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

import com.twitter.cassovary.graph.node.DynamicNode

/**
 * A class which supports dynamically adding or deleting edges.  Nodes are added dynamicly as
  * needed by edge additions.
 */
trait DynamicDirectedGraph[+V <: Node] extends DirectedGraph[V] {
  /**
   * Add an edge from {@code srcId} to {@code destId}.
   */
  def addEdge(srcId: Int, destId: Int): Unit

  /**
   * Remove an edge from a {@code srdId} to {@code destId}.
   * Return Option of source and destination nodes. None indicates the node doesn't exist in graph.
   */
  def removeEdge(srcId: Int, destId: Int): (Option[Node], Option[Node])
}
