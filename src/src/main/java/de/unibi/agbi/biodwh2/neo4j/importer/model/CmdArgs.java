package de.unibi.agbi.biodwh2.neo4j.importer.model;

import picocli.CommandLine;

@CommandLine.Command(name = "Neo4j-GraphML-Importer.jar", sortOptions = false, footer = "Visit https://github.com/BioDWH2/Neo4j-GraphML-Importer for more documentation.")
public class CmdArgs {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this message", order = 0)
    public boolean help;
    @CommandLine.Option(names = {
            "-i", "--input"
    }, arity = "1", paramLabel = "<graphMLFilePath>", description = "Path to the GraphML file", order = 1)
    public String inputFilePath;
    @CommandLine.Option(names = {
            "-e", "--endpoint"
    }, arity = "1", paramLabel = "<endpoint>", description = "Endpoint of a running Neo4j instance", order = 2)
    public String endpoint;
    @CommandLine.Option(names = {"--username"}, arity = "1", paramLabel = "<username>", description = "Neo4j username", order = 3)
    public String username;
    @CommandLine.Option(names = {"--password"}, arity = "1", paramLabel = "<password>", description = "Neo4j password", order = 4)
    public String password;
    @CommandLine.Option(names = {
            "--label-prefix"
    }, arity = "1", paramLabel = "<prefix>", description = "Prefix added to all node and edge labels", order = 5)
    public String labelPrefix;
    @CommandLine.Option(names = {
            "--indices"
    }, arity = "1", paramLabel = "<label1>.<property1>;<label2>.<property2>;...", description = "Create indices if not exist. Labels are not automatically prefixed!", order = 6)
    public String indices;
}
