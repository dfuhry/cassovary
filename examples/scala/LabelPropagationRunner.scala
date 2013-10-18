/*
 * Copyright 2012 Twitter, Inc.
 * Author: David Fuhry
 *
 */
/**
 * Loads VIT / Topic graph from a file and shows stats.
 *
 * Specifically, it loads an ArrayBasedDirectedGraph in using AdjacencyListGraphReader,
 * and configures the reader to use 2 threads to load the graph instead of just one.
 * This example loads in both toy_6nodes_adj_1.txt and toy_6nodes_adj_2.txt from
 * src/test/resources/graphs/
 */

import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, TestGraphs}
import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.cassovary.algorithms.LabelPropagation;
import com.twitter.cassovary.algorithms.LabelPropagationParams;
import com.twitter.util.Duration
import java.util.concurrent.Executors
import com.google.common.util.concurrent.MoreExecutors
import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList;
import scala.io.Source;
import java.io.PrintWriter;
import java.io.File;

object LabelPropagationRunner {
  def topicIdxToInt(tidxStr: String): Int = {
    if (tidxStr == "") -1
    else tidxStr.toInt
  }
    

  def main(args: Array[String]) {
    val dampenAmt = args(0).toDouble
    val pageRankIters = args(1).toInt

    val inputGraphFname = args(2)
    val uidSnFname = args(3)

    val inputGraphFile = new File(inputGraphFname)
    printf("inputGraphFile.getName(): %s\n", inputGraphFile.getName())
    val inputGraphParentFname = inputGraphFile.getParentFile().getPath()
    printf("inputGraphParentFname: %s\n", inputGraphParentFname)

    val uidSNLines = Source.fromFile(uidSnFname).getLines()
    val uidSNSplit = uidSNLines.map(_.split("\t")).map{case Array(s1, s2) => Pair(s1, s2) case _ => Pair("", "")}
    val uidSNMap = Map(uidSNSplit.toList: _ *)

    val graphReader = new AdjacencyListGraphReader(inputGraphParentFname, inputGraphFile.getName()) {
      override val executorService = MoreExecutors.sameThreadExecutor()
    }
    val graph = graphReader.toArrayBasedDirectedGraph()

    printf("Loaded graph loaded has %s nodes and %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)

    val labeledUsers = graph.filter{_.label != -1}
    val numLabeledUsers = labeledUsers.size
    val numTopics = graph.map{ node => node.label }.toStream.distinct.size

    printf("%d of the %d nodes (%f%%) are labeled\n", numLabeledUsers, graph.nodeCount, (numLabeledUsers.toDouble / graph.nodeCount.toDouble) * 100.0)

    val nodesAsList = graph.toList
  
    val NUM_FOLDS = 10

    var fold = 0
    while (fold < NUM_FOLDS) {
      val params = LabelPropagationParams(dampenAmt, Some(pageRankIters))
  
      // Cross-folds precision test.
      // Select kth 10th of labeled users.
      var holdOutUser = new Array[Boolean](graph.nodeCount)
      var labeledCt = 0
      val foldMinLabelCt = (fold * (numLabeledUsers.toDouble / (NUM_FOLDS))).toInt
      val foldMaxLabelCt = ((fold + 1) * (numLabeledUsers.toDouble / (NUM_FOLDS))).toInt
      holdOutUser.view.zipWithIndex.foreach { case (dummyHeldOut, hiIdx) =>  {
        if (nodesAsList(hiIdx).label != -1) {
          labeledCt = labeledCt + 1
          if (labeledCt >= foldMinLabelCt && labeledCt < foldMaxLabelCt) {
            holdOutUser(hiIdx) = true
          }
        }
      }}

      val numHeldOutUsers = holdOutUser.count(_ == true)
      printf("in fold %d, holding out %d of %d labeled users (%f%%, >= %d to < %d)\n", fold, numHeldOutUsers, numLabeledUsers, (numHeldOutUsers.toDouble / numLabeledUsers), foldMinLabelCt, foldMaxLabelCt)

      printf("LabelPropagationRunner instantiating LabelPropagation obj\n");
      val lp = LabelPropagation(graph, holdOutUser, params)
  
      val outFBase = inputGraphParentFname + "/dampenAmt_" + dampenAmt + "-pageRankIters_" + pageRankIters + "-fold_" + fold
      val outFname = outFBase + ".tsv"
      printf("writing propagation result to %s.\n", outFname)
  
      val writer = new PrintWriter(new File(outFname))
  
      lp.view.zipWithIndex.foreach{ case (nodeArr, node_idx) => {
	val uid = graphReader.vertexIdxToVertexId(node_idx)
        writer.printf("%s\t", int2Integer(uid))
        val screenName = uidSNMap(uid.toString)
        writer.printf("%s\t", screenName)
        val actualTopic = nodesAsList(node_idx).label
        writer.printf("%d\t", int2Integer(actualTopic))

	var trainingTopic = actualTopic
	if (holdOutUser(node_idx)) {
          trainingTopic = -1
	}

	writer.printf("%d\t", int2Integer(trainingTopic))
	val isHoldOutUser = holdOutUser(node_idx)
	writer.printf("%b\t", boolean2Boolean(isHoldOutUser))
        val nodeOutDegree = nodesAsList(node_idx).outboundCount
        writer.printf("%d\t", int2Integer(nodeOutDegree))
        nodeArr.view.zipWithIndex.foreach { case (weight, topic_idx) => {
          writer.printf("%.10f\t", double2Double(weight))
        }}
        writer.printf("\n")
      }}
  
      writer.close()

      // Evaluate precision of predicted labels for held-out users.
      var contingency = new Array[Array[Int]](numTopics)
      contingency.view.zipWithIndex.foreach{ case (dummyRow, topicIdx) => {
        contingency(topicIdx) = new Array[Int](numTopics)
      }}
      var predictedCorrect = 0
      val dampingAmount = (1.0D - dampenAmt) / graph.nodeCount
      lp.view.zipWithIndex.foreach{ case (nodeArr, nodeIdx) => {
        if (holdOutUser(nodeIdx)) {
          val actualTopic = nodesAsList(nodeIdx).label
          val predictedTopic = nodeArr.view.zipWithIndex.maxBy(_._1)._2
          if (predictedTopic == actualTopic) {
            predictedCorrect = predictedCorrect + 1
          }
          contingency(actualTopic)(predictedTopic) += 1
        }
      }}
      var predictedCorrect2 = (for (i <- 0 until numTopics) yield contingency(i)(i)).sum
      assert(predictedCorrect == predictedCorrect2)
      
      val outEvalFname = outFBase + ".eval"
      printf("writing evaluation result to %s\n", outEvalFname)

      val evalWriter = new PrintWriter(new File(outEvalFname))

      evalWriter.printf("Among %d held out users, %d topics (%f) predicted correct.\n", int2Integer(numHeldOutUsers), int2Integer(predictedCorrect), double2Double(predictedCorrect.toDouble / numHeldOutUsers))

      evalWriter.printf("confusion matrix:\n");

      evalWriter.printf("%d", int2Integer(0))
      for (j <- 1 until numTopics) {
        evalWriter.printf("\t%d", int2Integer(j))
      }
      evalWriter.printf(" <-- predicted as\n")
      for (i <- 0 until numTopics) {
        evalWriter.printf("%d", int2Integer(contingency(i)(0)))
        for (j <- 1 until numTopics) {
          evalWriter.printf("\t%d", int2Integer(contingency(i)(j)))
        }
        //evalWriter.printf("\t%d = %s", int2Integer(i), idxTopicMap.get(i).get)
        evalWriter.printf("\t%d = %d", int2Integer(i), int2Integer(i))
        evalWriter.printf("\n")
      }

      evalWriter.close()

      fold = fold + 1
    }

    printf("finished.\n")
  }
}
