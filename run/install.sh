#!/bin/bash

SERVICE=hbase-manager
VERSION=1.0

INSTALL_DIR=install-$SERVICE-$VERSION

cd ..

mvn clean

mvn assembly:assembly

mkdir -p ../$INSTALL_DIR
mkdir -p ../$INSTALL_DIR/log
cp target/$SERVICE-$VERSION-jar-with-dependencies.jar ../$INSTALL_DIR
cp -rf run ../$INSTALL_DIR/
cp -rf conf ../$INSTALL_DIR/

