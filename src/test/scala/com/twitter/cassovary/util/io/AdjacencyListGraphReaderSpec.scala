/*
 * Copyright 2012 Twitter, Inc.
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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.DirectedGraph
import java.util.concurrent.Executors
import org.specs.Specification

class AdjacencyListGraphReaderSpec extends Specification  {

  val nodeMap = Map( 10 -> List(11, 12, 13), 11 -> List(12, 14), 12 -> List(14),
    13 -> List(12, 14), 14 -> List(15), 15 -> List(10, 11))
  var graphReader: AdjacencyListGraphReader = _

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * and ensures that these are equivalent
   * @param g DirectedGraph
   * @param gr AdjacencyListGraphReader which can map between node remapped index and original id values.
   * @param nodeMap Map of node ids to ids of its neighbors
   */
  def nodeMapEquals(g:DirectedGraph, gr: AdjacencyListGraphReader, nodeMap: Map[Int, List[Int]]) = {
    g.foreach { node =>
      val nodeId = gr.vertexIdxToVertexId(node.id)
      nodeMap.contains(nodeId) mustBe true
      val neighbors = node.outboundNodes()
      val nodesInMap = nodeMap(nodeId)
      nodesInMap.foreach { i => { neighbors.contains(gr.nodeIdToNodeIdx(i)) mustBe true }}
      neighbors.foreach { i => { nodesInMap.contains(gr.nodeIdxToNodeId(i)) mustBe true }}
    }
    nodeMap.keys.foreach { id => g.existsNodeId(gr.nodeIdToNodeIdx(id)) mustBe true }
  }

  var graph: DirectedGraph = _

  "AdjacencyListReader" should {

    doBefore{
      // Example using 2 threads to read in the graph
      graphReader = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes_adj") {
          override val executorService = Executors.newFixedThreadPool(2)
      }
      graph = graphReader.toSharedArrayBasedDirectedGraph()
    }

    "provide the correct graph properties" in {
      graph.nodeCount mustBe 6
      graph.edgeCount mustBe 11L
      graph.maxNodeId mustBe 5
    }

    "contain the right nodes and edges" in {
      nodeMapEquals(graph, graphReader, nodeMap)
    }

  }

}
