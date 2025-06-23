name := "business-integration-data-challenge"

organization := "com.axelspringer"
organizationName := "Axel Springer"
organizationHomepage := Some(url("https://www.axelspringer.com"))

version := "1.0"

scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.0"  excludeAll(
    ExclusionRule(organization = "org.apache.avro"))
)

