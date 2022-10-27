引入哪些依赖：stream的依赖。
https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/try-flink/datastream/

```
cd /mnt
cd d
cd ideaProjects/

mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-scala \
    -DarchetypeVersion=1.16.0 \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```
