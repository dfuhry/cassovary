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
import net.lag.logging.Logger
import com.twitter.cassovary.util.Progress
import java.io.{File,PrintWriter}

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param iterations How many PageRank iterations do you want?
 */
case class LabelPropagationParams(dampingFactor: Double = 0.85,
                          iterations: Option[Int] = Some(10))

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
  def apply(graph: DirectedGraph, uidxTidx: Array[Int], params: LabelPropagationParams): Array[Array[Double]] = {
    val lp = new LabelPropagation(graph, uidxTidx, params)
    lp.run
  }

  /**
   * Execute a single iteration of label propagation, given the previous label propagation matrix
   * @param graph A DirectedGraph instance
   * @param params LabelPropagationParams
   * @param prArray A matrix of doubles, with rows corresponding to node ids and columns corresponding to topics.
   * @return The updated array
   */
  def iterate(graph: DirectedGraph, idxToTopic: Array[Int], params: LabelPropagationParams, prMatrix: Array[Array[Double]]) = {
    val lp = new LabelPropagation(graph, idxToTopic, params)
    printf("LabelPropagation iterate calling iterate. Total memory: %d\n", Runtime.getRuntime.freeMemory())    
    lp.iterate(prMatrix: Array[Array[Double]])
  }
}

private class LabelPropagation(graph: DirectedGraph, uidxTidx: Array[Int], params: LabelPropagationParams) {

  private val log = Logger.get("LabelPropagation")

  val dampingFactor = params.dampingFactor
  //val dampingAmount = (1.0D - dampingFactor) / graph.nodeCount
  val numTopics = uidxTidx.reduceLeft(_ max _) + 1
  val dampingAmount = new Array[Double](numTopics)
  for ((dummyAmt, i) <- dampingAmount.view.zipWithIndex) dampingAmount(i) = uidxTidx.count(_ == i).toDouble / graph.nodeCount.toDouble;

  /**
   * Execute Label Propagation with the desired params
   * @return A matrix of Label Propagation values
   */
  def run: Array[Array[Double]] = {

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of Label Propagation by renumbering this graph!")

    var beforePR = new Array[Array[Double]](graph.maxNodeId + 1);
    // Populate each row slice with 1 for column of user's topic, zeros otherwise.
    //for ((nodeArr, i) <- beforePR.view.zipWithIndex) beforePr(i) = new Array[Double](uidxTidx.size, 0.0); 
    //for ((nodeArr, i) <- beforePR.view.zipWithIndex) beforePr(i)(uidxTidx(i)) = 1.0;
    
    log.info("Initializing LabelPropagation...")
    val progress = Progress("pagerank_init", 65536, Some(graph.nodeCount))
    //val initialPageRankValue = 1.0D / graph.nodeCount
    graph.foreach { node =>
      //beforePR(node.id) = initialPageRankValue
      //printf("LabelPropagation.run allocating array of size %d for node %d\n", numTopics, node.id)
      beforePR(node.id) = new Array[Double](numTopics); 
      if (node.id >= uidxTidx.size) {
        printf("Error: node.id %d >= uidxTidx.size %d\n", node.id, uidxTidx.size);
        val minNodeId = graph.map { case node => node.id }.reduceLeft(_ min _)
        printf("min node.id: %d\n", minNodeId)
	val maxNodeId = graph.map { case node => node.id }.reduceLeft(_ max _)
	printf("max node.id: %d\n", maxNodeId)
        
      }
      if (uidxTidx(node.id) != -1) {
	val topicCount = uidxTidx.count(_ == uidxTidx(node.id))
	//printf("topic count for topic %d: %d\n", uidxTidx(node.id), topicCount)
        beforePR(node.id)(uidxTidx(node.id)) = 1.0D / topicCount;
      }
      progress.inc
    }

    val convergFname = "label_propagation_results/dampenAmt_" + params.dampingFactor + "-pageRankIters_" + params.iterations.get + ".converg.tsv"
    val convergWriter = new PrintWriter(new File(convergFname))
    var afterPR = beforePR
    (0 until params.iterations.get).foreach { i =>
      log.info("Beginning %sth iteration".format(i))
      afterPR = iterate(beforePR)
      convergWriter.printf("%d\t%.12f\n", int2Integer(i), double2Double(convergence(beforePR, afterPR)))
      convergWriter.flush()
      beforePR = afterPR
    }
    convergWriter.close()

    beforePR
  }

  /**
   * Execute a single iteration of PageRank on the input array
   * @param beforePR PageRank values before the iteration
   * @return PageRank values after the iteration
   */
  def iterate(beforePR: Array[Array[Double]]) = {
    val numTopics = beforePR(0).size
    val afterPR = new Array[Array[Double]](graph.maxNodeId + 1)
    for ((afterPRRow, i) <- afterPR.view.zipWithIndex) afterPR(i) = new Array[Double](numTopics)

    log.info("Calculating new Label Propagation values based on previous iteration...")
    val progress = Progress("pagerank_calc", 65536, Some(graph.nodeCount))
    graph.foreach { node =>
      //val givenPageRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      //val givenTopicRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      //beforePR(node.id).view.zipWithIndex.foreach { case (weight, topic_idx) =>
      //  beforePR(node.id)(topic_idx) /= node.neighborCount(GraphDir.OutDir)
      //}
      val givenTopicRank = beforePR(node.id) map (weight => weight / node.neighborCount(GraphDir.OutDir))
      node.neighborIds(GraphDir.OutDir).foreach { neighborId =>
        //afterPR(neighborId) += givenPageRank
        //afterPR(neighborId) += givenTopicRank
	//printf("processing node %d neighborId %d\n", node.id, neighborId);
	afterPR(neighborId).view.zipWithIndex.foreach { case (weight, topic_idx) =>
	  afterPR(neighborId)(topic_idx) += givenTopicRank(topic_idx)
	}
      }
      progress.inc
    }

    //printf("Frobenius norm before damping: %.10f\n", convergence(beforePR, afterPR))

    log.info("Damping...")
    val progress_damp = Progress("pagerank_damp", 65536, Some(graph.nodeCount))
    //if (dampingAmount > 0) {
      graph.foreach { node =>
        //afterPR(node.id) = dampingAmount + dampingFactor * afterPR(node.id)
	afterPR(node.id).view.zipWithIndex.foreach { case (weight, topic_idx) => 
	  //afterPR(node.id)(topic_idx) = dampingAmount + dampingFactor * afterPR(node.id)(topic_idx)
	  // TODO: make first parameter the topic prior.
	  afterPR(node.id)(topic_idx) = (1.0 - dampingFactor) * dampingAmount(topic_idx) + dampingFactor * afterPR(node.id)(topic_idx)
	}
        progress_damp.inc
      }
      //printf("Frobenius norm after damping: %.10f\n", convergence(beforePR, afterPR))
    //}


    afterPR
  }

  // Compute Frobenius norm (sum of squares of elementwise differences) between two matrices.
  def convergence(beforePR: Array[Array[Double]], afterPR: Array[Array[Double]]) = {
    var ssq = 0.0;
    beforePR.view.zipWithIndex.foreach { case (nodeArr, node_idx) =>
      nodeArr.view.zipWithIndex.foreach { case (weight, topic_idx) => 
        ssq += math.pow(weight - afterPR(node_idx)(topic_idx), 2)
      }
    }
    ssq
  }

}

