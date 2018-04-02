    name := "MapReducer"
     
    version := "1.0"
     
    scalaVersion := "2.11.7"

    val liftVersion = "3.0-RC3"

    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.0" % "test"

    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.2" % "test"

//    libraryDependencies += "org.typelevel" %% "cats-core" % "0.4.1"

    libraryDependencies +="net.liftweb"       %% "lift-mapper"        % liftVersion        % "compile"

