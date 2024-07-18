#!/bin/sh
java -cp "config:lib/*:proline-cli-${pom.version}.jar" -Dlogback.configurationFile=config/logback.xml fr.proline.cli.ProlineCLI $@
