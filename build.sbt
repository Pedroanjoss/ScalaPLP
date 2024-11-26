ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "ProjetoPLP",
    libraryDependencies ++= sparkDependencies ++ plotlyDependencies
  )

// Dependências do Spark
lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// Dependências do Plotly-Scala
lazy val plotlyDependencies = Seq(
  "org.plotly-scala" %% "plotly-render" % "0.8.5",
  "org.plotly-scala" %% "plotly-jupyter-scala" % "0.4.0",
  "org.plotly-scala" %% "plotly-almond" % "0.8.2"
)
