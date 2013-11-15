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
import scala.collection.mutable.{ArrayBuffer,HashMap,SynchronizedMap}

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
  override def equals(other: Any): Boolean =  {
    other match {
      case that: NodeIntLabel => 
        (intLabel == that.intLabel)
      case _ => false
    }
  }
  override def toString = intLabel.toString
}


class SimpleNodeLabelParser extends NodeLabelParser {
  def parseLabelStr(labelStr: String): NodeLabel = {
    new NodeIntLabel(labelIdToLabelIdx.getOrElseUpdate(labelStr, labelIdToLabelIdx.size))
  }
}


class NodeIntSetLabel (var intLabelSet: Array[Int]) extends NodeLabel {
  override def equals(other: Any): Boolean = {
    other match {
      case that: NodeIntSetLabel =>
        intLabelSet.equals(that.intLabelSet)
      case _ => false
    }
  }
  override def toString = intLabelSet.toString
}


class SetNodeLabelParser extends NodeLabelParser {

  def parseLabelStr(labelStr: String): NodeLabel = {
    new NodeIntSetLabel(labelStr.split("\\s+").map{ label =>
      labelIdToLabelIdx.getOrElseUpdate(label, labelIdToLabelIdx.size)
    }.toStream.distinct.toArray)
  }

}


class WeightedSetNodeLabel (var weightedLabelSet: Array[Pair[Int,Double]]) extends NodeLabel {
  override def equals(other: Any): Boolean = {
    other match {
      case that: WeightedSetNodeLabel =>
        weightedLabelSet.equals(that.weightedLabelSet)
      case _ => false
    }
  }
  override def toString = weightedLabelSet.toString
}


class WeightedSetNodeLabelParser extends NodeLabelParser {
  
  def parseLabelStr(labelStr: String) = {
    var lblTokens = labelStr.split("\\s+")
    assert(lblTokens.size % 2 == 0)
    var labelWeightPairs: ArrayBuffer[Pair[Int,Double]] = new ArrayBuffer[Pair[Int,Double]]()
    for (i <- 0 until lblTokens.size / 2) {
      val labelId = lblTokens(i * 2)
      val labelIdx = labelIdToLabelIdx.getOrElseUpdate(labelId, labelIdToLabelIdx.size)
      labelWeightPairs += Pair(labelIdx, lblTokens(i * 2 + 1).toDouble)
    }
    new WeightedSetNodeLabel(labelWeightPairs.toArray)
  }

}

