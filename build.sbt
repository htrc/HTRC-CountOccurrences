import Dependencies._

showCurrentGitBranch

git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization := "org.hathitrust.htrc",
  organizationName := "HathiTrust Research Center",
  organizationHomepage := Some(url("https://www.hathitrust.org/htrc")),
  scalaVersion := "2.12.12",
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-language:postfixOps",
    "-language:implicitConversions"
  ),
  externalResolvers ++= Seq(
    Resolver.defaultLocal,
    Resolver.mavenLocal,
    "HTRC Nexus Repository" at "https://nexus.htrc.illinois.edu/content/groups/public"
  ),
  packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
    ("Git-Sha", git.gitHeadCommit.value.getOrElse("N/A")),
    ("Git-Branch", git.gitCurrentBranch.value),
    ("Git-Version", git.gitDescribedVersion.value.getOrElse("N/A")),
    ("Git-Dirty", git.gitUncommittedChanges.value.toString),
    ("Build-Date", new java.util.Date().toString)
  )
)

lazy val ammoniteSettings = Seq(
  libraryDependencies +=
    {
      val version = scalaBinaryVersion.value match {
        case "2.10" => "1.0.3"
        case _ => "2.2.0"
      }
      "com.lihaoyi" % "ammonite" % version % Test cross CrossVersion.full
    },
  sourceGenerators in Test += Def.task {
    val file = (sourceManaged in Test).value / "amm.scala"
    IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
    Seq(file)
  }.taskValue,
  fork in (Test, run) := false
)

lazy val `count-occurrences` = (project in file("."))
  .enablePlugins(GitVersioning, GitBranchPrompt, JavaAppPackaging)
  .settings(commonSettings)
  .settings(ammoniteSettings)
  .settings(spark("3.0.1"))
//  .settings(spark_dev("3.0.1"))
  .settings(
    name := "count-occurrences",
    description := "Counts the number of times each of the given keywords occurs in the given set" +
      "of volumes and writes the result to a CSV file.",
    licenses += "Apache2" -> url("http://www.apache.org/licenses/LICENSE-2.0"),
    libraryDependencies ++= Seq(
      "org.hathitrust.htrc"           %% "data-model"           % "1.8.1",
      "org.hathitrust.htrc"           %% "scala-utils"          % "2.10.1",
      "org.hathitrust.htrc"           %% "spark-utils"          % "1.3",
      "org.rogach"                    %% "scallop"              % "3.5.1",
      "com.gilt"                      %% "gfc-time"             % "0.0.7",
      "com.github.nscala-time"        %% "nscala-time"          % "2.26.0",
      "ch.qos.logback"                %  "logback-classic"      % "1.2.3",
      "org.codehaus.janino"           %  "janino"               % "2.7.8",  // must be this version; 3.x has conflicts
      "org.scalacheck"                %% "scalacheck"           % "1.15.1"      % Test,
      "org.scalatest"                 %% "scalatest"            % "3.2.3"       % Test
    )
    ,
    dependencyOverrides ++= Seq(
      "com.google.guava" % "guava" % "15.0",
    )
  )
