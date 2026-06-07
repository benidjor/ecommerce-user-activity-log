ThisBuild / scalaVersion := "2.13.17"  // 설치된 brew spark의 Scala 버전과 일치
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.activitylog"

val sparkVersion = "4.1.2"  // Task 0에서 확인한 brew spark 버전과 일치시킬 것

// JDK 17에서 Spark 실행에 필요한 add-opens
val jvm17Opens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "activity-log",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.18"     % Test
    ),
    // provided 의존성을 테스트 컴파일/실행 클래스패스에 포함
    Test / unmanagedClasspath ++= (Compile / managedClasspath).value,
    Test / fork := true,
    Test / javaOptions ++= jvm17Opens,
    Test / parallelExecution := false,
    run / fork := true,
    run / javaOptions ++= jvm17Opens,
    // thin jar(sbt package)로 spark-submit. assembly는 README용 fat jar(선택)
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )
