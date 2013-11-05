/*
 * Copyright 2013 Twitter, Inc.
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

import com.twitter.cassovary.graph.node.NodeLabel
import scala.util.matching.Regex
import scala.collection.mutable.{HashMap,SynchronizedMap}

trait NodeLabelParser {
  val labelIdToLabelIdx = new HashMap[String,Int] with SynchronizedMap[String,Int]
  var lazyLabelIdxToLabelId: Array[String] = Array[String]()

  def parseLabelStr(labelStr: String): NodeLabel

  /**
   * Returns original label string given label index.
   * Builds reverse map if not already built.
   */
  def labelIdxToLabelId(labelIdx: Int): String = {
    if (lazyLabelIdxToLabelId.isEmpty) {
      lazyLabelIdxToLabelId = new Array[String](labelIdToLabelIdx.size)
      labelIdToLabelIdx.foreach { case (labelId, labelIdx) => lazyLabelIdxToLabelId(labelIdx) = labelId }
    }
    lazyLabelIdxToLabelId(labelIdx)
  }

  def numDistinctLabels: Int = {
    labelIdToLabelIdx.size
  }
}


class NodeIntLabel (var intLabel: Int) extends NodeLabel {
  
}


class SimpleNodeLabelParser extends NodeLabelParser {
  def parseLabelStr(labelStr: String): NodeLabel = {
    new NodeIntLabel(labelIdToLabelIdx.getOrElseUpdate(labelStr, labelIdToLabelIdx.size))
  }


}


class NodeIntSetLabel (var intLabelSet: Array[Int]) extends NodeLabel {
  
}


class SetNodeLabelParser extends NodeLabelParser {

  def parseLabelStr(labelStr: String): NodeLabel = {
    new NodeIntSetLabel(labelStr.split("\\s+").map{ label =>
      labelIdToLabelIdx.getOrElseUpdate(label, labelIdToLabelIdx.size)
    }.toStream.distinct.toArray)
  }

}
