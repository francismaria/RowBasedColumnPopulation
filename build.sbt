val scala3Version = "2.12.0"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.12" % "3.2.2",
"org.apache.spark" % "spark-sql_2.12" % "3.2.2"
)
libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
