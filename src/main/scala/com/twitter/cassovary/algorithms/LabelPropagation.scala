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
package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.{DirectedGraph, GraphDir}
import com.twitter.cassovary.graph.LabeledNode
import com.twitter.cassovary.util.io.{WeightedSetNodeLabelParser,WeightedSetNodeLabel}
import net.lag.logging.Logger
import com.twitter.cassovary.util.Progress
import java.io.{File,PrintWriter}
import java.util.Arrays
import scala.util.control.Breaks._

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param iterations How many PageRank iterations do you want?
 */
case class LabelPropagationParams(dampingFactor: Double = 0.85,
                          iterations: Option[Int] = Some(10),
			  truncThreshold: Double = 0.0,
			  convergEpsilon: Double = 0.0,  // 0.0 interpreted as "never terminate based on convergence"
			  maxTruncationId: Int = Int.MaxValue  // Only nodes whose id is <= this value will be considered for truncation.
			  )

/**
 * PageRank is a link analysis algorithm designed to measure the importance of nodes in a graph.
 * Popularized by Google.
 *
 * Unoptimized for now, and runs in a single thread.
 */
object LabelPropagation {

  /**
   * Execute label propagation with PageRank-like approach.
   * Note that the memory usage of this implementation is
   * proportional to the graph's maxId - you might want to renumber the
   * graph before running PageRank.
   * @param graph A DirectedGraph instance
   * @param params LabelPropagationParams from above
   * @return An array of doubles, with indices corresponding to node ids
   */
  def apply(graph: DirectedGraph, nodeLabelParser: WeightedSetNodeLabelParser, params: LabelPropagationParams): Array[Double] = {
    val lp = new LabelPropagation(graph, nodeLabelParser, params)
    lp.run
  }

  /**
   * Execute a single iteration of label propagation, given the previous label propagation matrix
   * @param graph A DirectedGraph instance
   * @param params LabelPropagationParams
   * @param prArray A matrix of doubles, with rows corresponding to node ids and columns corresponding to topics.
   * @param afArray A matrix of doubles, same format as prArray, for writing into.
   * @return The updated array
   */
  def iterate(graph: DirectedGraph, nodeLabelParser: WeightedSetNodeLabelParser, params: LabelPropagationParams, prMatrix: Array[Double], afMatrix: Array[Double]) = {
    val lp = new LabelPropagation(graph, nodeLabelParser, params)
    lp.iterate(0, prMatrix: Array[Double], afMatrix: Array[Double])
  }
}

private class LabelPropagation(graph: DirectedGraph, nodeLabelParser: WeightedSetNodeLabelParser, params: LabelPropagationParams) {

  private val log = Logger.get("LabelPropagation")

  val dampingFactor = params.dampingFactor
  val dampingAmount = (1.0D - dampingFactor) / graph.nodeCount
  val nodeLabels: Array[WeightedSetNodeLabel] = graph.view.map(_.asInstanceOf[LabeledNode].label.asInstanceOf[WeightedSetNodeLabel]).toArray
  //val numTopics = nodeLabels.reduceLeft(_ max _) + 1
  //val numTopics = graph.maxLabelIdx + 1
  val numTopics = nodeLabelParser.numDistinctLabels
  //val topicCt = graph.view.map(_.label).foldLeft(scala.collection.mutable.MutableList[Int](numTopics)){ (tc, v) => tc.update(v, tc(v) + 1) }
  //val topicCt: Array[Int] = new Array[Int](numTopics)  // Populated in run method.
  //graph.view.groupBy(_.label).mapValues(_.size)

  /**
   * Execute Label Propagation with the desired params
   * @return A matrix of Label Propagation values
   */
  def run: Array[Double] = {

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of Label Propagation by renumbering this graph!")

    //printf("in run, numTopics: %d, topicCt.size: %d\n", numTopics, topicCt.size)
    printf("in run, numTopics: %d\n", numTopics)

    /*
    val topicCtMap = graph.view.groupBy(_.label).mapValues(_.size)
    (0 to numTopics - 1) foreach { topicIdx =>
      topicCt(topicIdx) = topicCtMap.getOrElse(topicIdx, 0)
      printf("for topic id %d, assigned count as %d\n", topicIdx, topicCt(topicIdx))
    }
    printf("sum of all topic counts: %d, graph.nodeCount: %d\n", topicCt.sum, graph.nodeCount)
    */

    //val beforePR = new Array[Array[Double]](graph.maxNodeId + 1);
    var beforePR = new Array[Double]((graph.maxNodeId + 1) * numTopics)
    //Arrays.fill(beforePR, dampingAmount)
    
    log.info("Initializing LabelPropagation...")
    val progress = Progress("LabelPropagation_init", 262144, Some(graph.nodeCount))
    //val initialPageRankValue = 1.0D / graph.nodeCount

    (0 to graph.nodeCount - 1) foreach { nodeId => 
      //var nodeTopicDistr = new Array[Double](numTopics); 
      val nodeLabel = nodeLabels(nodeId)
      if (nodeLabel != null) {
        nodeLabel.weightedLabelSet.foreach { case(topicIdx, userTopicWeight) =>
          //nodeTopicDistr(nodeLabel) = 1.0D / topicCt(nodeLabel);
          //beforePR(nodeId * numTopics + nodeLabel) = 1.0D / topicCt(nodeLabel)
          //beforePR(nodeId * numTopics + topicIdx) = 1.0D
          beforePR(nodeId * numTopics + topicIdx) = userTopicWeight
        }
      }
      //beforePR(nodeId) = nodeTopicDistr
      progress.inc
    }

    var afterPR = new Array[Double]((graph.maxNodeId + 1) * numTopics)
    var converged = false
    breakable { (0 until params.iterations.get).foreach { i =>
      log.info("LabelPropagation.run beginning %sth iteration".format(i))
      iterate(i, beforePR, afterPR)

      val converg = convergence(beforePR, afterPR)
      printf("Convergence after iteration %d: %.12f\n", i, converg)
      if (params.convergEpsilon > 0.0D) {
	if (converg < params.convergEpsilon) {
	  printf("Convergence after iteration %d: %f < convergEpsilon %f. Breaking.\n", i, converg, params.convergEpsilon)
          break
	} else {
          printf("Convergence after iteration %d: %f > convergEpsilon %f. Continuing.\n", i, converg, params.convergEpsilon)
	}
      }

      // Swap before and after arrays.
      var tempPR = beforePR
      beforePR = afterPR
      afterPR = tempPR

      Arrays.fill(afterPR, 0.0D)
    } }

    beforePR
  }

  /**
   * Execute a single iteration of PageRank on the input array
   * @param beforePR PageRank values before the iteration
   * @return PageRank values after the iteration
   */
  def iterate(iteration: Int, beforePR: Array[Double], afterPR: Array[Double]) = {
    // TODO: Pass numTopics properly or make property of graph.
    //val numTopics = beforePR.size / (graph.maxNodeId + 1)
    printf("iterate using numTopics %d\n", numTopics)
    printf("maxTruncationId: %d\n", params.maxTruncationId)

    //for ((afterPRRow, i) <- afterPR.view.zipWithIndex) afterPR(i) = new Array[Double](numTopics)

    log.info("Calculating new Label Propagation values based on previous iteration...")
    val progress = Progress("LabelPropagation_calc", 65536, Some(graph.nodeCount))
    val givenTopicRank: Array[Double] = new Array(numTopics);
    graph.foreach { node =>
      //val givenTopicRank = beforePR(node.id) map (weight => weight / node.neighborCount(GraphDir.OutDir))
      //val givenTopicRank = { for (nodeTopicIdx <- (node.id * numTopics) until ((node.id + 1) * numTopics)) yield beforePR(nodeTopicIdx) / node.neighborCount(GraphDir.OutDir) }

      val nodeTopicIdx = node.id * numTopics
      if (node.id <= params.maxTruncationId) {
        for (topicIdx <- 0 until numTopics) {
          givenTopicRank(topicIdx) = beforePR(nodeTopicIdx + topicIdx) / node.neighborCount(GraphDir.OutDir)
        }
      } else {
        // List Renormalization: list based on fraction of each topic associated with it.
	val topicValsSum = beforePR.slice(nodeTopicIdx, nodeTopicIdx + numTopics).sum
	assert(!topicValsSum.isNaN())
        for (topicIdx <- 0 until numTopics) {
          var gtr = 0.0D
          val beforeTopicVal = beforePR(nodeTopicIdx + topicIdx)
          if (beforeTopicVal != 0.0D) {
            gtr = beforeTopicVal * (beforeTopicVal / topicValsSum) / node.neighborCount(GraphDir.OutDir).toDouble
            assert(!gtr.isNaN())
          }
          givenTopicRank(topicIdx) = gtr
        }
      }


      node.neighborIds(GraphDir.OutDir).foreach { neighborId =>
	val neighborTopicIdx = neighborId * numTopics
	for (topicIdx <- 0 until numTopics) {
          afterPR(neighborTopicIdx + topicIdx) += givenTopicRank(topicIdx)
	}
      }
      progress.inc
    }

    //printf("Frobenius norm before damping: %.10f\n", convergence(beforePR, afterPR))

    // Only truncate when 1st part (users) of bipartite nodes receive weight.
    if (iteration % 2 == 1 && params.truncThreshold > 0.0) {
      /*
      log.info("Normalizing...")
      val progress_norm = Progress("LabelPropagation_norm", 65536, Some(graph.nodeCount))
      graph.foreach { node => 
	val nodeWgtSum = afterPR.slice(node.id * numTopics, (node.id + 1) * numTopics).sum
	if (nodeWgtSum > 0.0D) {
	  val nodeTopicIdx = node.id * numTopics
          for (topicIdx <- 0 until numTopics) {
            afterPR(nodeTopicIdx + topicIdx) /= nodeWgtSum
	  }
	}
        progress_norm.inc
      }
      */


      log.info("Truncating...")
      val progress_trunc = Progress("pagerank_trunc", 65536, Some(graph.nodeCount))
      var numTruncated = 0
      /*
      val topicWgtPercentile: Array[Double] = new Array(numTopics)
      //val topicTruncThreshold: Array[Double] = { for (topicIdx <- 0 until numTopics) yield (1.0D / topicCt(topicIdx)) * params.truncThreshold }.toArray
      for (topicIdx <- 0 until numTopics) {
	val topicVals: Array[Double] = { for (nodeIdx <- 0 to params.maxTruncationId)
          yield afterPR(nodeIdx * numTopics + topicIdx)
	}.toArray
	Arrays.sort(topicVals)
	var i = 0
	while (topicVals(i) == 0.0D) {
          i = i + 1
	}
        val percentileIdx: Int = i + ((params.maxTruncationId - i) * params.truncThreshold).toInt
        printf("topicIdx: %d, truncThreshold: %f, percentileIdx: %d\n", topicIdx, params.truncThreshold, percentileIdx)
        topicWgtPercentile(topicIdx) = topicVals(percentileIdx)
	printf("Topic %d: min %.12f, max %.12f, of %d at percentile %f: %.12f\n", topicIdx, topicVals(0), topicVals(topicVals.size - 1), topicVals.size, params.truncThreshold, topicWgtPercentile(topicIdx))
      }
      */

      for (nodeIdx <- 0 to params.maxTruncationId) {
        val nodeTopicOffset = nodeIdx * numTopics
        //val nodeSumWgt = afterPR.slice(nodeTopicOffset, nodeTopicOffset + numTopics).sum
        for (topicIdx <- 0 until numTopics) {
          val nodeTopicWgt = afterPR(nodeTopicOffset + topicIdx)
          // topicTruncThreshold(topicIdx)
          //val topicRatio = topicCt(node.label).toDouble / graph.nodeCount
          //if (nodeTopicWgt < (nodeSumWgt * params.truncThreshold)) {
          //if (nodeTopicWgt < (1.0D / topicCt(node.label).toDouble) * params.truncThreshold) {
          //if (nodeTopicWgt < topicTruncThreshold(topicIdx)) {
          if (nodeTopicWgt < params.truncThreshold) {
          //if (nodeTopicWgt != 0.0D && nodeTopicWgt < topicWgtPercentile(topicIdx)) {
            //printf("Truncating node %d topic %d with value %f < %f\n", node.id, topicIdx, afterPR(nodeTopicOffset + topicIdx), topicTruncThreshold(topicIdx))
            afterPR(nodeTopicOffset + topicIdx) = 0.0D
            numTruncated = numTruncated + 1
          }
        }
        progress_trunc.inc
      }
      printf("Truncated %d of %d (%f%%)\n", numTruncated, (params.maxTruncationId + 1) * numTopics, (numTruncated.toDouble / ((params.maxTruncationId + 1) * numTopics).toDouble) * 100.0D)
    }

    if (dampingAmount > 0) {
      log.info("Damping...")
      printf("Applying dampingAmount %.12f\n", dampingAmount)
      val progress_damp = Progress("pagerank_damp", 65536, Some(graph.nodeCount))
      graph.foreach { node =>
	/*
	afterPR(node.id).view.zipWithIndex.foreach { case (weight, topic_idx) => 
	  //afterPR(node.id)(topic_idx) = dampingAmount + dampingFactor * afterPR(node.id)(topic_idx)
	  afterPR(node.id)(topic_idx) = (1.0 - dampingFactor) * dampingAmount(topic_idx) + dampingFactor * afterPR(node.id)(topic_idx)
	}
	*/
	for (nodeTopicIdx <- (node.id * numTopics) until ((node.id + 1) * numTopics)) {
	  afterPR(nodeTopicIdx) = dampingAmount + dampingFactor * afterPR(nodeTopicIdx)
	}
        progress_damp.inc
      }
      //printf("Frobenius norm after damping: %.10f\n", convergence(beforePR, afterPR))
    }
    
  }

  // Compute Frobenius norm (sum of squares of elementwise differences) between two arrays.
  def convergence(beforePR: Array[Double], afterPR: Array[Double]) = {
    var ssq = 0.0;
    for (nodeTopicIdx <- 0 until beforePR.size) {
      val diff = beforePR(nodeTopicIdx) - afterPR(nodeTopicIdx)
      val sqr = math.pow(diff, 2)
      ssq += sqr
    }
    ssq
  }

}

