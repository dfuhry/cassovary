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

import com.twitter.cassovary.graph.ArrayBasedDirectedGraph
import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import com.twitter.cassovary.graph.node.ArrayBasedDirectedNode
import java.io.File
import scala.io.Source
import scala.collection.mutable.{Map,SynchronizedMap,HashMap}

/**
 * Reads in a multi-line adjacency list from multiple files in a directory.
 * Does not check for duplicate edges or nodes.
 *
 * You can optionally specify which files in a directory to read. For example, you may have files starting with
 * "part-" that you'd like to read. Only these will be read in if you specify that as the file prefix.
 *
 * In each file, a node and its neighbors is defined by the first line being that
 * node's id and its # of neighbors, followed by that number of ids on subsequent lines.
 * The number of ids can optionally be followed by a double-quoted node label string.
 * For example,
 *    241 3 "News"
 *    2
 *    4
 *    1
 *    53 1 "Sports"
 *    241
 *    ...
 * In this file, node 241 has 3 neighbors, namely 2, 4 and 1. Node 53 has 1 neighbor, 241.
 * Node 241's label is "News". Node 53's label is "Sports".
 *
 * @param directory the directory to read from
 * @param prefixFileNames the string that each part file starts with
 */
class AdjacencyListGraphReader (directory: String, prefixFileNames: String = "") extends GraphReader {

  val nodeIdToNodeIdx = new HashMap[Int,Int] with SynchronizedMap[Int,Int]
  val labelIdToLabelIdx = new HashMap[String,Int] with SynchronizedMap[String,Int]
  val nodeIdxToNodeId = new HashMap[Int,Int]
  val labelIdxToLabelId = new HashMap[Int,String]

  /**
   * Read in nodes and edges from a single file
   * @param filename Name of file to read from
   */
  class OneShardReader(filename: String, nodeIdToNodeIdx: SynchronizedMap[Int,Int], labelIdToLabelIdx: SynchronizedMap[String,Int]) extends Iterator[NodeIdEdgesMaxId] {

    private val outEdgePattern = """^(\d+)\s+(\d+)(?:\s+\"(.*)\")?""".r
    private val lines = Source.fromFile(filename).getLines()
    private val holder = NodeIdEdgesMaxId(-1, null, -1, -1)

    override def hasNext: Boolean = lines.hasNext

    override def next(): NodeIdEdgesMaxId = {
      val outEdgePattern(id, outEdgeCount, labelStr) = lines.next.trim
      var i = 0
      val outEdgeCountInt = outEdgeCount.toInt

      val idInt = nodeIdToNodeIdx.getOrElseUpdate(id.toInt, nodeIdToNodeIdx.size)

      var newMaxId = idInt
      val outEdgesArr = new Array[Int](outEdgeCountInt)
      while (i < outEdgeCountInt) {
        val edgeId = lines.next.trim.toInt

        val edgeInt = nodeIdToNodeIdx.getOrElseUpdate(edgeId.toInt, nodeIdToNodeIdx.size)

        newMaxId = newMaxId max edgeInt
        outEdgesArr(i) = edgeInt
        i += 1
      }
      val labelIdx = labelIdToLabelIdx.getOrElseUpdate(labelStr, labelIdToLabelIdx.size)

      holder.id = idInt
      holder.edges = outEdgesArr
      holder.maxId = newMaxId
      holder.label = labelIdx
      holder
    }

  }

  /**
   * Read in nodes and edges from multiple files
   * @param directory Directory to read from
   * @param prefixFileNames the string that each part file starts with
   */
  class ShardsReader(directory: String, prefixFileNames: String = "", nodeIdToNodeIdx: SynchronizedMap[Int,Int], labelIdToLabelIdx: SynchronizedMap[String,Int]) {
    val dir = new File(directory)

    def readers: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
      val validFiles = dir.list().flatMap({ filename =>
        if (filename.startsWith(prefixFileNames)) {
          Some(filename)
        }
        else {
          None
        }
      })
      validFiles.map({ filename =>
      {() => new OneShardReader(directory + "/" + filename, nodeIdToNodeIdx, labelIdToLabelIdx)}
      }).toSeq
    }
  }

  def iteratorSeq = {
    new ShardsReader(directory, prefixFileNames, nodeIdToNodeIdx, labelIdToLabelIdx).readers
  }

  /**
   * Returns original vertex identifier given (remapped) vertex index.
   * Builds reverse map if not already built.
   */
  def vertexIdxToVertexId(nodeIdx: Int): Int = {
    if (nodeIdxToNodeId.isEmpty) {
      (nodeIdxToNodeId /: nodeIdToNodeIdx.map{ case (nodeId, nodeIdx) => (nodeIdx, nodeId) })(_ += _)
    }
    nodeIdxToNodeId(nodeIdx)
  }

  def labelIdxToLabelId(labelIdx: Int): Int = {
    if (labelIdxToLabelId.isEmpty) {
      (labelIdxToLabelId /: labelIdToLabelIdx.map{ case (labelId, labelIdx) => (labelIdx, labelId) })(_ += _)
    }
    labelIdxToLabelId(labelIdx)
  }

  /*
  def unRenumberedGraph(graph: ArrayBasedDirectedGraph): Int = {
    // graph.toList.foreach{ node => node.id = labelIdxToLabelId(node.id) }
    val unRenumberedNodes = graph.toList.view.map{ node => ArrayBasedDirectedNode(labelIdxToLabelId(node.id), node.label, node.neighbors, node.dir) }
    val executorService = MoreExecutors.sameThreadExecutor()
    ArrayBasedDirectedGraph(List(unRenumberedNodes), executorService, graph.storedGraphDir)
  }
  */

}
