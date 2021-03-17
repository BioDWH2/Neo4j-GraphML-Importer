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
    @CommandLine.Option(names = {
            "--username"
    }, arity = "1", paramLabel = "<username>", description = "Neo4j username", order = 3)
    public String username;
    @CommandLine.Option(names = {
            "--password"
    }, arity = "1", paramLabel = "<password>", description = "Neo4j password", order = 4)
    public String password;
    @CommandLine.Option(names = {
            "--label-prefix"
    }, arity = "1", paramLabel = "<prefix>", description = "Prefix added to all node and edge labels", order = 5)
    public String labelPrefix;
    @CommandLine.Option(names = {
            "--label-suffix"
    }, arity = "1", paramLabel = "<suffix>", description = "Suffix added to all node and edge labels", order = 6)
    public String labelSuffix;
    @CommandLine.Option(names = {
            "--modify-node-labels"
    }, arity = "1", paramLabel = "<true|false>", defaultValue = "true", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "Flag if node labels should be modified with prefix and suffix", order = 7)
    public String modifyNodeLabels;
    @CommandLine.Option(names = {
            "--modify-edge-labels"
    }, arity = "1", paramLabel = "<true|false>", defaultValue = "true", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, description = "Flag if edge labels should be modified with prefix and suffix", order = 8)
    public String modifyEdgeLabels;
    @CommandLine.Option(names = {
            "--indices"
    }, arity = "1", paramLabel = "<label1>.<property1>;<label2>.<property2>;...", description = "Create indices if not exist. Prefix and suffix are not automatically added to these labels!", order = 9)
    public String indices;
}
