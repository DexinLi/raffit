name := "Raft"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.9.2",
  "org.rocksdb" % "rocksdbjni" % "5.5.1",
  "com.twitter" %% "scrooge-core" % "17.11.0" exclude("com.twitter", "libthrift"),
  "com.twitter" %% "finagle-thrift" % "17.11.0" exclude("com.twitter", "libthrift")
)