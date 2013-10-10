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
import scala.io.Source;
import java.io.PrintWriter;
import java.io.File;

object LabelPropagationRunner {
  def topicIdxToInt(tidxStr: String): Int = {
    if (tidxStr == "") -1
    else tidxStr.toInt
  }
    

  def main(args: Array[String]) {
    val idxUidMap = Map(Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.uid_map").getLines().zipWithIndex.map{ case (s,i) => (i,s) }.toList: _ *)
    val idxTopicMap = Map(Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.topic1_t1id_map").getLines().zipWithIndex.map{ case (s,i) => (i,s) }.toList: _ *)

    var uidxTidx = (Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.uincrid_t1incrid").getLines() map ( tidxStr => topicIdxToInt(tidxStr) )).toArray
    //{ case ("") => -1 case (s) => s.toInt }.toList;
    val numLabeledUsers = uidxTidx.count(_ != -1)
    val topicDistrUnnorm = uidxTidx.filter(v => v != -1).foldLeft[Map[Int,Int]](Map.empty)((m, c) => m + (c-> (m.getOrElse(c, 0) + 1)))
    printf("topicDistrUnnorm: %s\n", topicDistrUnnorm.toString())
    val topicDistrNorm = Map[Int,Double](topicDistrUnnorm.view.map{ case (topicIdx, topicCt) => (topicIdx, topicCt.toDouble / numLabeledUsers.toDouble) }.toList: _ *)
    printf("topicDistrNorm: %s\n", topicDistrNorm.toString())


    val uidSNLines = Source.fromFile("/Users/dfuhry/gssl/dfuhry_kfu_users_ids").getLines()
    val uidSNSplit = uidSNLines.map(_.split("\t")).map{case Array(s1, s2) => Pair(s1, s2) case _ => Pair("", "")}
    val uidSNMap = Map(uidSNSplit.toList: _ *)

    val graph = new AdjacencyListGraphReader("/Users/dfuhry/gssl/", "dfuhry-kfu-edges-7_cassovary_adj_") {
      //override val executorService = Executors.newFixedThreadPool(4)
      //override val executorService = Executors.newSingleThreadExecutor()
      override val executorService = MoreExecutors.sameThreadExecutor()
    }.toArrayBasedDirectedGraph()

    printf("Loaded graph loaded has %s nodes and %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)

    printf("%d of the %d nodes (%f%%) are labeled\n", numLabeledUsers, uidxTidx.size, (numLabeledUsers.toDouble / uidxTidx.size.toDouble) * 100.0)

    val NUM_FOLDS = 10

    var dampenAmt = 0.05;
    while (dampenAmt <= 0.50) {
      //var pageRankIters = 0;
      var pageRankIters = 2;
      while (pageRankIters <= 8) {
	var fold = 0
	while (fold < NUM_FOLDS) {
          val params = LabelPropagationParams(dampenAmt, Some(pageRankIters))
  
          // TODO: implement cross-folds precision experiment here.
          // Select kth 10th of labeled users.
          var holdOutUser = new Array[Boolean](graph.nodeCount)
	  var labeledCt = 0
	  val foldMinLabelCt = (fold * (numLabeledUsers.toDouble / (NUM_FOLDS))).toInt
	  val foldMaxLabelCt = ((fold + 1) * (numLabeledUsers.toDouble / (NUM_FOLDS))).toInt
	  holdOutUser.view.zipWithIndex.foreach { case (dummyHeldOut, hiIdx) =>  {
	    if (uidxTidx(hiIdx) != -1) {
	      labeledCt = labeledCt + 1
	      if (labeledCt >= foldMinLabelCt && labeledCt < foldMaxLabelCt) {
	        holdOutUser(hiIdx) = true
	      }
	    }
	  }}

	  val numHeldOutUsers = holdOutUser.count(_ == true)
	  printf("in fold %d, holding out %d of %d labeled users (%f%%, >= %d to < %d)\n", fold, numHeldOutUsers, numLabeledUsers, (numHeldOutUsers.toDouble / numLabeledUsers), foldMinLabelCt, foldMaxLabelCt)

	  // Strip labels from trainingUidxTidx where holdOutUser(i) == True
	  // TODO: determine which users are true VITs (of the 55K) and just consider them.
	  val trainingUidxTidx = uidxTidx.clone()
	  trainingUidxTidx.view.zipWithIndex.foreach { case(dummyTidx, userIdx) => {
            if (holdOutUser(userIdx)) {
              trainingUidxTidx(userIdx) = -1
	    }
	  }}
  
          printf("LabelPropagationRunner instantiating LabelPropagation obj\n");
          val lp = LabelPropagation(graph, trainingUidxTidx, params)
      
          /*
          lp.zipWithIndex.sortWith(_._1 < _._1).foreach { case(prVal, idx) =>  {
              val id = idxUidMap.get(idx).get;
              //printf("PageRank: %f\n", prVal);
              //printf("Id: %s\n", id);
              //printf("ScreenName: %s\n", uidSNMap.get(id).get);
              printf("%f\t%s\n", prVal, uidSNMap.get(id).get);
            }
          }
          */
      
          val outFname = "label_propagation_results/dampenAmt_" + dampenAmt + "-pageRankIters_" + pageRankIters + ".tsv"
          printf("writing propagation result to %s.\n", outFname)
      
          val writer = new PrintWriter(new File(outFname))
      
          val nodesAsList = graph.toList
      
          lp.view.zipWithIndex.foreach{ case (nodeArr, node_idx) => {
            //writer.printf("%d\t", int2Integer(node_idx));
            val uid = idxUidMap.get(node_idx).get
            writer.printf("%s\t", uid)
            val screenName = uidSNMap.get(uid).get
            writer.printf("%s\t", screenName)
            val nodeOutDegree = nodesAsList(node_idx).outboundCount
            writer.printf("%d\t", int2Integer(nodeOutDegree))
            nodeArr.view.zipWithIndex.foreach { case (weight, topic_idx) => {
              writer.printf("%.10f\t", double2Double(weight))
            }}
            writer.printf("\n")
          }}
      
          writer.close()

	  // TODO: evaluate precision of predicted labels for held-out users.
	  var predictedCorrect = 0
	  var predictedCorrect2 = 0
	  lp.view.zipWithIndex.foreach{ case (nodeArr, nodeIdx) => {
	    if (holdOutUser(nodeIdx)) {
	      val actualTopic = uidxTidx(nodeIdx)
              val predictedTopic = nodeArr.view.zipWithIndex.maxBy(_._1)._2
	      if (predictedTopic == actualTopic) {
                predictedCorrect = predictedCorrect + 1
	      }
	      // Normalize by topic prior.
	      var predictedTopic2 = nodeArr.view.zipWithIndex.maxBy(v => v._1 / topicDistrNorm.get(v._2).get)._2
	      if (predictedTopic2 == actualTopic) {
                predictedCorrect2 = predictedCorrect2 + 1
	      }
	    }
	  }}
	  
	  printf("Without topic normalizing, among %d held out users, %d topics (%f) predicted correct.\n", numHeldOutUsers, predictedCorrect, (predictedCorrect.toDouble / numHeldOutUsers))
	  printf("With topic normalizing, among %d held out users, %d topics (%f) predicted correct.\n", numHeldOutUsers, predictedCorrect2, (predictedCorrect2.toDouble / numHeldOutUsers))
    
	  fold = fold + 1
	}

        printf("increasing pageRankIters from %d", pageRankIters);
        if (pageRankIters == 0) {
          pageRankIters = pageRankIters + 1
        } else {
          pageRankIters = pageRankIters * 2
        }
        printf(" to %d\n", pageRankIters);

      }
      dampenAmt = dampenAmt + 0.05
    }

    printf("finished.\n")
  }
}
