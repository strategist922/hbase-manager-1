#!/bin/bash

SERVICE=hbase-manager
VERSION=1.0

function usage() {
    echo "sh run.sh <type>"
    echo "type: HBaseWrite / HBaseStabilityTester / CreateHTable / HBaseSelect / HBaseCount / HBaseDelete / HBaseOperate / HBaseDump / HBaseLoadTable"
}


if [ $# -lt 1 ]; then
    usage
    exit 1
fi

if [ $1 == "HBaseWriter" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseWriter"
elif [ $1 == "HBaseStabilityTester" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseStabilityTester"
elif [ $1 == "CreateHTable" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.CreateHTable"
elif [ $1 == "HBaseSelect" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseSelect"
elif [ $1 == "HBaseCount" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseCount"
elif [ $1 == "HBaseDelete" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseDelete"
elif [ $1 == "HBaseOperate" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseOperate"
elif [ $1 == "HBaseDump" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseDump"
elif [ $1 == "HBaseLoadTable" ];then
    CLASS_FILE="com.wandoujia.hbase.manager.HBaseLoadTable"
fi

PARAS="$2"


cd ..
CLASSPATH=${CLASSPATH}:./conf
CLASSPATH=${CLASSPATH}:./$SERVICE-$VERSION-jar-with-dependencies.jar
export CLASSPATH
mkdir ./log >/dev/null 2>&1
java -Xms128m -Xmx1024m $CLASS_FILE $PARAS
