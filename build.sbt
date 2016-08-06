import Dependencies._

showCurrentGitBranch

git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization := "org.hathitrust.htrc",
  organizationName := "HathiTrust Research Center",
  organizationHomepage := Some(url("https://www.hathitrust.org/htrc")),
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-feature", "-language:postfixOps", "-language:implicitConversions", "-target:jvm-1.7"),
  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
  resolvers ++= Seq(
    "I3 Repository" at "http://nexus.htrc.illinois.edu/content/groups/public",
    Resolver.mavenLocal
  ),
  packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
    ("Git-Sha", git.gitHeadCommit.value.getOrElse("N/A")),
    ("Git-Branch", git.gitCurrentBranch.value),
    ("Git-Version", git.gitDescribedVersion.value.getOrElse("N/A")),
    ("Git-Dirty", git.gitUncommittedChanges.value.toString),
    ("Build-Date", new java.util.Date().toString)
  )
)

lazy val `count-occurrences` = (project in file(".")).
  enablePlugins(GitVersioning, GitBranchPrompt, JavaAppPackaging).
  settings(commonSettings: _*).
  settings(spark("2.0.0"): _*).
  //settings(spark_dev("2.0.0"): _*).
  settings(
    name := "count-occurrences",
    version := "1.0-SNAPSHOT",
    description := "Counts the number of times each of the given keywords occurs in the given set" +
      "of volumes and writes the result to a CSV file.",
    licenses += "Apache2" -> url("http://www.apache.org/licenses/LICENSE-2.0"),
    libraryDependencies ++= Seq(
      "org.hathitrust.htrc"           %% "pairtree-to-text"     % "3.1.1",
      "edu.illinois.i3.scala"         %% "scala-utils"          % "1.1",
      "org.rogach"                    %% "scallop"              % "2.0.0",
      "com.jsuereth"                  %% "scala-arm"            % "1.4",
      "com.gilt"                      %% "gfc-time"             % "0.0.5",
      "com.github.nscala-time"        %% "nscala-time"          % "2.12.0",
      "org.scalatest"                 %% "scalatest"            % "2.2.6"      % Test
    )
  )
