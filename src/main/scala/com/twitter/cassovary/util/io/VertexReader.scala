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

import com.twitter.cassovary.graph.{NodeIdEdgesMaxIdTrait,NodeIdEdgesMaxId,LabeledNodeIdEdgesMaxId,NodeRenumberer}
import scala.collection.mutable.{SynchronizedMap,HashMap}
import scala.util.matching.Regex

abstract class VertexReader(val nodeRenumberer: NodeRenumberer) {
  def outEdgePattern: Regex
  def vertexLineToHolder(line: String): NodeIdEdgesMaxIdTrait
}


class SimpleVertexReader(override val nodeRenumberer: NodeRenumberer) extends VertexReader(nodeRenumberer) {
  val _holder: NodeIdEdgesMaxId = NodeIdEdgesMaxId(-1, null, -1)
  val outEdgePattern: Regex = """^(\d+)\s+(\d+)""".r

  def vertexLineToHolder(line: String) = {
    val outEdgePattern(id, outEdgeCount) = line
    _holder.id = nodeRenumberer.nodeIdToNodeIdx(id.toInt) 
    val outEdgeCountInt = outEdgeCount.toInt
    _holder.edges = new Array[Int](outEdgeCountInt)
    _holder.maxId = -1
    _holder
  }
}


class LabeledVertexReader(override val nodeRenumberer: NodeRenumberer, val nodeLabelParser: NodeLabelParser) extends VertexReader(nodeRenumberer) {
  val _holder: LabeledNodeIdEdgesMaxId = LabeledNodeIdEdgesMaxId(-1, null, -1, null)
  val outEdgePattern: Regex = """^(\d+)\s+(\d+)(?:\s+\"(.*)\")?""".r


  def vertexLineToHolder(line: String) = {
    val outEdgePattern(id, outEdgeCount, labelStr) = line
    _holder.id = nodeRenumberer.nodeIdToNodeIdx(id.toInt)
    _holder.label = null
    if (labelStr != null) {
      _holder.label = nodeLabelParser.parseLabelStr(labelStr)
    }
    val outEdgeCountInt = outEdgeCount.toInt
    _holder.edges = new Array[Int](outEdgeCountInt)
    _holder.maxId = -1
    _holder
  }

}


trait VertexReaderFactory {
  def newInstance(nodeRenumberer: NodeRenumberer): VertexReader
}

class SimpleVertexReaderFactory extends VertexReaderFactory {
  def newInstance(nodeRenumberer: NodeRenumberer) = {
    new SimpleVertexReader(nodeRenumberer)
  }
}

class LabeledVertexReaderFactory (val nodeLabelParser: NodeLabelParser) extends VertexReaderFactory {
  def newInstance(nodeRenumberer: NodeRenumberer) = {
    new LabeledVertexReader(nodeRenumberer, nodeLabelParser)  
  }
}

