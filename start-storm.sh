#!/bin/sh

echo "============*******************================="
echo "========= 自动发布topology到storm集群 =========="
echo "============*******************================="

git pull origin master

filepath=$(cd "$(dirname "$0")"; pwd)
jsonFile="$filepath/$1"
mvn clean package -Dmaven.test.skip=ture
storm jar target/storm-multilang-0.0.1-jar-with-dependencies.jar cn.seeking.multilang.StormMultilang $jsonFile