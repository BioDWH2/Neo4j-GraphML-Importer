package de.unibi.agbi.biodwh2.neo4j.importer.model;

import picocli.CommandLine;

@CommandLine.Command(name = "Neo4j-GraphML-Importer.jar")
public class CmdArgs {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this message")
    public boolean help;
    @CommandLine.Option(names = {
            "-i", "--input"
    }, arity = "1", paramLabel = "<graphMLFilePath>", description = "Path to the GraphML file")
    public String inputFilePath;
    @CommandLine.Option(names = {
            "-e", "--endpoint"
    }, arity = "1", paramLabel = "<endpoint>", description = "Endpoint of a running Neo4j instance")
    public String endpoint;
    @CommandLine.Option(names = {"--username"}, arity = "1", paramLabel = "<username>", description = "Neo4j username")
    public String username;
    @CommandLine.Option(names = {"--password"}, arity = "1", paramLabel = "<password>", description = "Neo4j password")
    public String password;
}
