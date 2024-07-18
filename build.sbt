
val prolineVersion = "2.1.0" // "2.1.2-SNAPSHOT"
val hqVersion = "2.4.7.Final"

ThisBuild / scalaVersion       := "2.11.12"
ThisBuild / crossScalaVersions := List("2.11.12")
ThisBuild / version            := "0.3.0-SNAPSHOT"
ThisBuild / organization       := "com.github.profiproteomics"
ThisBuild / organizationName   := "ProFI"

lazy val root = (project in file("."))
  .settings(
    name := "proline-cli",

    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.1",
      "com.github.pureconfig" %% "pureconfig" % "0.14.0",
      "commons-io" % "commons-io" % "2.5", // used for resursive delete

      // Proline dependencies
      "fr.profi.util" %% "profi-commons-scala" % "1.0.0-SNAPSHOT",
      "fr.proline" % "proline-databases" % prolineVersion,
      "fr.proline" %% "proline-om" % prolineVersion,
      "fr.proline" %% "proline-dal" % prolineVersion,
      "fr.proline" %% "proline-omp" % prolineVersion,
      "fr.proline" % "proline-admin" % "0.9.0",
      "fr.proline" % "pm-mascot-parser" % "0.9.0",
      "matrixscience" % "msparser-native-libraries-2.5.2" % "0.9.0",
      "matrixscience" % "msparser" % "2.5.2" classifier "Windows_amd64",
      "fr.proline" % "pm-maxquant-parser" % "0.5.0",
      "fr.proline" % "pm-omssa-parser" % "0.9.0",
      "fr.proline" % "pm-xtandem-parser" % "0.8.0",
      "fr.proline" % "pm-dataset-exporter" % "0.9.0",
      "fr.proline" % "pm-fragmentmatch-generator" % "0.9.0",
      "fr.proline" % "pm-msdiag" % "0.9.0",
      "fr.proline" % "pm-mzidentml" % "0.9.0",
      
      // Required for quantitation => check if still needed when using next core version
      "com.github.davidmoten" % "flatbuffers-java" % "1.9.0.1",
      
      // JNR (used to call native libraries)
      "com.github.jnr" % "jffi" % "1.3.2",
      "com.github.jnr" % "jnr-ffi" % "2.2.1",

      // JSONITER!
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"   % "2.13.3",
      // Use the "provided" scope instead when the "compile-internal" scope is not supported
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.13.3" % "provided", // "compile-internal"
      
      // Workaround for http://stackoverflow.com/questions/21882100/adding-hornetq-dependency-in-sbt-gives-resolution-failure-for-hornetq-native-n
      "org.hornetq" % "hornetq-native" % hqVersion from s"https://repo1.maven.org/maven2/org/hornetq/hornetq-native/$hqVersion/hornetq-native-$hqVersion.jar",
      // TODO: upgrade the Scala Client to use the latest Cortex API
      ("fr.proline" % "proline-cortex-scalaclient" % "0.5.0-SNAPSHOT" changing()).exclude("org.hornetq","hornetq-native"),
      
      // Workaround for https://stackoverflow.com/questions/42855218/exception-in-thread-main-java-lang-noclassdeffounderror-org-dom4j-io-staxeven
      "dom4j" % "dom4j" % "1.6.1"
    ),

    // Exclude some dependencies
    libraryDependencies ~= { _.map { dependency =>
      dependency
        // TODO: we have to exclude this specific dependency because of existing version conflict not managed by SBT
        .exclude("javax.persistence", "persistence-api")
      }
    },

    resolvers += Resolver.mavenLocal,
    /*resolvers ++= Seq(
      "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
    )*/

    // -- SBT assembly settings -- //
    mainClass in assembly := Some("fr.proline.cli.ProlineCLI"),
    assemblyJarName in assembly := "proline-cli.jar", // TODO: try to find a way to tag with version number
    /*assemblyMergeStrategy in assembly := {
      //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "persistence.xml") => MergeStrategy.first
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }*/

    // See: https://stackoverflow.com/questions/46287789/running-an-uber-jar-from-sbt-assembly-results-in-error-could-not-find-or-load-m
    assemblyMergeStrategy in assembly := {
      case "module-info.class" => MergeStrategy.discard
      case n if n.endsWith("blueprint.xml") => MergeStrategy.discard
      case PathList("log4j.properties", xs @ _*) => MergeStrategy.discard // jmzidentml and pride-jaxb
      // FIXME: duplicated entries of config classes should be removed from latest trunk on github
      case PathList("fr", "proline", "core", "algo", "lcms", xs @ _*) => MergeStrategy.discard
      case PathList("fr", "proline", "core", "algo", "msq", "summarizing", xs @ _*) => MergeStrategy.first // added for BuildMasterQuantPeptide
      case PathList("fr", "proline", "core", "orm", xs @ _*) => MergeStrategy.first
      case PathList("antlr", xs @ _*) => MergeStrategy.first
      case PathList("com", "sun", xs @ _*) => MergeStrategy.first
      case PathList("javax", xs @ _*) => MergeStrategy.first
      case PathList("org", xs @ _*) => MergeStrategy.first
      case x => {
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
      }
    }
  )
