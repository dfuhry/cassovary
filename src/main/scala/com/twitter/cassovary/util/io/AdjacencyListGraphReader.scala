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

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import java.io.File
import scala.io.Source
import scala.collection.mutable.Map;
import scala.collection.mutable.SynchronizedMap;

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

  /**
   * Read in nodes and edges from a single file
   * @param filename Name of file to read from
   */
  class OneShardReader(filename: String, nodeIdxMap: SynchronizedMap, labelIdxMap: SynchronizedMap) extends Iterator[NodeIdEdgesMaxId] {

    private val outEdgePattern = """^(\d+)\s+(\d+)(?:\s+\"(.*)\")?""".r
    private val lines = Source.fromFile(filename).getLines()
    private val holder = NodeIdEdgesMaxId(-1, null, -1, -1)

    override def hasNext: Boolean = lines.hasNext

    override def next(): NodeIdEdgesMaxId = {
      val outEdgePattern(id, outEdgeCount, labelStr) = lines.next.trim
      var i = 0
      val outEdgeCountInt = outEdgeCount.toInt

      val idInt = nodeIdxMap.getOrElseUpdate(id.toInt, nodeIdxMap.size)

      var newMaxId = idInt
      val outEdgesArr = new Array[Int](outEdgeCountInt)
      while (i < outEdgeCountInt) {
        val edgeId = lines.next.trim.toInt

        val edgeInt = nodeIdxMap.getOrElseUpdate(edgeId.toInt, nodeIdxMap.size)

        newMaxId = newMaxId max edgeInt
        outEdgesArr(i) = edgeInt
        i += 1
      }
      val labelIdx = labelIdxMap.getOrElseUpdate(labelStr, labelIdxMap.size)

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
  class ShardsReader(directory: String, prefixFileNames: String = "") {
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
      val nodeIdxMap = SynchronizedMap[Int,Int]()
      val labelIdxMap = SynchronizedMap[String,Int]()
      validFiles.map({ filename =>
      {() => new OneShardReader(directory + "/" + filename, nodeIdxMap, labelIdxMap)}
      }).toSeq
    }
  }

  def iteratorSeq = {
    new ShardsReader(directory, prefixFileNames).readers
  }

}
