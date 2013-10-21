#!/bin/bash

BUILD_PACKAGE="build"

root=$(
  cd $(dirname $0)/../..
  echo $PWD
)
echo root is $root
cd $root

if [ $BUILD_PACKAGE = build ]; then
  echo Downloading dependent jars...
  $root/sbt update
  echo Building Cassovary jar...
  $root/sbt package
  SBT_PACKAGE_RET=$?
  if [ $SBT_PACKAGE_RET -ne 0 ]; then
    echo "Error: Building Cassovary jar failed with exit code $SBT_PACKAGE_RET"
    exit $SBT_PACKAGE_RET
  fi
fi

JAVA_CP=(
  $(find $root/target -name 'cassovary*.jar') \
  $(ls -t -1 $HOME/.sbt/boot/scala-*/lib/scala-library.jar | head -1) \
  $(find $root/lib_managed/jars/ -name '*.jar')
)
JAVA_CP=$(echo ${JAVA_CP[@]} | tr ' ' ':')

cd examples/scala
mkdir -p classes
rm -rf classes/*
echo Compiling LabelPropagationRunner...
scalac -cp $JAVA_CP -d classes LabelPropagationRunner.scala
SCALAC_RET=$?
if [ $SCALAC_RET -ne 0 ]; then
  echo "Error: scalac failed with exit code $SCALAC_RET"
  exit $SCALAC_RET
fi

echo Running LabelPropagationRunner...
JAVA_OPTS="-server -Xmx15g -Xms1g"
java ${JAVA_OPTS} -cp $JAVA_CP:classes LabelPropagationRunner "$@"