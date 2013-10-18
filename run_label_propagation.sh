#!/bin/bash

# Exit on any error.
set -e

INPUT_GRAPH_FNAME="/Users/dfuhry/gssl/kfu-7-percentile33/edges.txt"
UID_SN_FNAME="/Users/dfuhry/gssl/dfuhry_kfu_users_ids"

examples/scala/label_propagation.sh 0.1 1 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.1 2 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.1 3 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.1 4 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.1 8 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.1 0 $INPUT_GRAPH_FNAME $UID_SN_FNAME

examples/scala/label_propagation.sh 0.2 1 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.2 2 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.2 3 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.2 4 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.2 8 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.2 0 $INPUT_GRAPH_FNAME $UID_SN_FNAME

examples/scala/label_propagation.sh 0.05 1 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.05 2 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.05 3 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.05 4 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.05 8 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.05 0 $INPUT_GRAPH_FNAME $UID_SN_FNAME

examples/scala/label_propagation.sh 0.3 1 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.3 2 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.3 3 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.3 4 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.3 8 $INPUT_GRAPH_FNAME $UID_SN_FNAME
examples/scala/label_propagation.sh 0.3 0 $INPUT_GRAPH_FNAME $UID_SN_FNAME

