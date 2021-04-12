package de.unibi.agbi.biodwh2.neo4j.importer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.unibi.agbi.biodwh2.neo4j.importer.model.CmdArgs;
import de.unibi.agbi.biodwh2.neo4j.importer.model.GithubAsset;
import de.unibi.agbi.biodwh2.neo4j.importer.model.GithubRelease;
import de.unibi.agbi.biodwh2.neo4j.importer.model.Version;
import de.unibi.agbi.biodwh2.neo4j.importer.model.graphml.PropertyKey;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class Neo4jGraphImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphImporter.class);
    private static final String RELEASE_URL = "https://api.github.com/repos/BioDWH2/Neo4j-GraphML-Importer/releases";
    private static final int BATCH_SIZE = 1000;
    private static final Version NEW_INDEX_CREATION_NEO4J_VERSION = new Version(4, 1, 3);
    private static final long TRANSACTION_SIZE = 20000;

    private Neo4jGraphImporter() {
    }

    public static void main(final String... args) {
        final CmdArgs commandLine = parseCommandLine(args);
        new Neo4jGraphImporter().run(commandLine);
    }

    private static CmdArgs parseCommandLine(final String... args) {
        final CmdArgs result = new CmdArgs();
        final CommandLine cmd = new CommandLine(result);
        cmd.parseArgs(args);
        return result;
    }

    private void run(final CmdArgs commandLine) {
        checkForUpdate();
        if (StringUtils.isNotEmpty(commandLine.inputFilePath) && StringUtils.isNotEmpty(commandLine.endpoint))
            importGraphML(commandLine.inputFilePath, commandLine.endpoint, commandLine.username, commandLine.password,
                          parseLabelOptions(commandLine), parseIndices(commandLine.indices));
        else {
            LOGGER.error("Input and endpoint arguments must be specified");
            printHelp(commandLine);
        }
    }

    private void checkForUpdate() {
        final Version currentVersion = getCurrentVersion();
        Version mostRecentVersion = null;
        String mostRecentDownloadUrl = null;
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final URL releaseUrl = new URL(RELEASE_URL);
            final List<GithubRelease> releases = mapper.readValue(releaseUrl, new TypeReference<List<GithubRelease>>() {
            });
            for (final GithubRelease release : releases) {
                final Version version = Version.tryParse(release.tagName.replace("v", ""));
                if (version != null) {
                    final String jarName = "Neo4j-GraphML-Importer-" + release.tagName + ".jar";
                    final Optional<GithubAsset> jarAsset = release.assets.stream().filter(
                            asset -> asset.name.equalsIgnoreCase(jarName)).findFirst();
                    if (jarAsset.isPresent() && mostRecentVersion == null || version.compareTo(mostRecentVersion) > 0) {
                        mostRecentVersion = version;
                        //noinspection OptionalGetWithoutIsPresent
                        mostRecentDownloadUrl = jarAsset.get().browserDownloadUrl;
                    }
                }
            }
        } catch (IOException | ClassCastException ignored) {
        }
        if (currentVersion == null && mostRecentVersion != null || currentVersion != null && currentVersion.compareTo(
                mostRecentVersion) < 0) {
            LOGGER.info("=======================================");
            LOGGER.info("New version " + mostRecentVersion + " of Neo4j-GraphML-Importer is available at:");
            LOGGER.info(mostRecentDownloadUrl);
            LOGGER.info("=======================================");
        }
    }

    private Version getCurrentVersion() {
        try {
            final Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                try {
                    final Manifest manifest = new Manifest(resources.nextElement().openStream());
                    final Version version = Version.tryParse(manifest.getMainAttributes().getValue("BioDWH2-version"));
                    if (version != null)
                        return version;
                } catch (IOException ignored) {
                }
            }
        } catch (IOException ignored) {
        }
        return null;
    }

    private LabelOptions parseLabelOptions(final CmdArgs commandLine) {
        final LabelOptions result = new LabelOptions();
        final String modifyNodeLabelsSafe =
                commandLine.modifyNodeLabels == null ? "" : commandLine.modifyNodeLabels.trim();
        final String modifyEdgeLabelsSafe =
                commandLine.modifyEdgeLabels == null ? "" : commandLine.modifyEdgeLabels.trim();
        result.modifyNodeLabels = !"false".equalsIgnoreCase(modifyNodeLabelsSafe) && !"0".equalsIgnoreCase(
                modifyNodeLabelsSafe);
        result.modifyEdgeLabels = !"false".equalsIgnoreCase(modifyEdgeLabelsSafe) && !"0".equalsIgnoreCase(
                modifyEdgeLabelsSafe);
        result.prefix = commandLine.labelPrefix;
        result.suffix = commandLine.labelSuffix;
        return result;
    }

    private Map<String, List<String>> parseIndices(final String indicesInput) {
        final Map<String, List<String>> indices = new HashMap<>();
        if (indicesInput != null) {
            final String[] parts = StringUtils.split(indicesInput, ';');
            for (final String part : parts) {
                final String[] labelPropertyKeyParts = StringUtils.split(part, '.');
                if (labelPropertyKeyParts.length == 2) {
                    final String label = StringUtils.stripStart(labelPropertyKeyParts[0], ":");
                    if (!indices.containsKey(label))
                        indices.put(label, new ArrayList<>());
                    indices.get(label).add(labelPropertyKeyParts[1]);
                } else {
                    LOGGER.warn("Failed to parse index '" + part + "' will be ignored. Ensure the syntax " +
                                "<label1>.<property1>;<label2>.<property2>;...");
                }
            }
        }
        return indices;
    }

    private void printHelp(final CmdArgs commandLine) {
        CommandLine.usage(commandLine, System.out);
    }

    private void importGraphML(final String inputFilePath, final String endpoint, final String username,
                               final String password, final LabelOptions labelOptions,
                               final Map<String, List<String>> indices) {
        if (!Paths.get(inputFilePath).toFile().exists()) {
            LOGGER.error("Input file '" + inputFilePath + "' not found");
            return;
        }
        final Path inputFile = Paths.get(inputFilePath);
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Parsing property definitions...");
        AtomicLong nodeCount = new AtomicLong();
        AtomicLong edgeCount = new AtomicLong();
        handleAllElementsInXMLWithTag(inputFile, "node", (reader, startElement) -> nodeCount.getAndIncrement());
        handleAllElementsInXMLWithTag(inputFile, "edge", (reader, startElement) -> edgeCount.getAndIncrement());
        LOGGER.info(nodeCount.get() + " nodes, " + edgeCount.get() + " edges");
        final Map<String, PropertyKey> propertyKeyNameMap = new HashMap<>();
        handleAllElementsInXMLWithTag(inputFile, "key", (reader, startElement) -> {
            final PropertyKey property = getPropertyKeyFromElement(startElement);
            propertyKeyNameMap.put(property.forType + "|" + property.id, property);
        });
        try (final Driver driver = GraphDatabase.driver(endpoint, getAuthToken(username, password))) {
            try (final Session session = driver.session()) {
                final Version neo4jVersion = getNeo4jKernelVersion(session);
                final Map<String, Long> nodeIdNeo4jIdMap = importAllNodes(session, inputFile, labelOptions,
                                                                          propertyKeyNameMap, nodeCount);
                importAllEdges(session, inputFile, labelOptions, propertyKeyNameMap, edgeCount, nodeIdNeo4jIdMap);
                createIndices(neo4jVersion, session, indices);
            }
        }
    }

    private void handleAllElementsInXMLWithTag(final Path inputFilePath, final String tagName,
                                               final Callback<XMLEventReader, StartElement> callback) {
        try (final FileInputStream stream = new FileInputStream(inputFilePath.toFile())) {
            final XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(stream);
            while (reader.hasNext()) {
                final XMLEvent nextEvent = tryNextEvent(reader);
                if (nextEvent != null && nextEvent.isStartElement()) {
                    final StartElement startElement = nextEvent.asStartElement();
                    if (startElement.getName().getLocalPart().equals(tagName))
                        callback.callback(reader, startElement);
                }
            }
        } catch (IOException | XMLStreamException e) {
            LOGGER.error("Failed to load GraphML", e);
        }
    }

    private XMLEvent tryNextEvent(final XMLEventReader reader) {
        try {
            return reader.nextEvent();
        } catch (XMLStreamException e) {
            LOGGER.warn("Failed to read XML event", e);
            return null;
        }
    }

    private PropertyKey getPropertyKeyFromElement(final StartElement element) {
        final String id = getElementAttribute(element, "id");
        final String forType = getElementAttribute(element, "for");
        final String attributeName = getElementAttribute(element, "attr.name");
        final String attributeList = getElementAttribute(element, "attr.list");
        final String attributeType = getElementAttribute(element, "attr.type");
        return new PropertyKey(id, forType, attributeName, attributeType, attributeList);
    }

    private String getElementAttribute(final StartElement element, final String name) {
        final Attribute attribute = element.getAttributeByName(QName.valueOf(name));
        return attribute != null ? attribute.getValue() : null;
    }

    private AuthToken getAuthToken(final String username, final String password) {
        return StringUtils.isEmpty(username) ? AuthTokens.none() : AuthTokens.basic(username, password);
    }

    private Node parseNode(final XMLEventReader reader, final StartElement element,
                           final Map<String, PropertyKey> propertyKeyNameMap, final LabelOptions labelOptions) {
        Node result = new Node();
        result.id = getElementAttribute(element, "id");
        result.labels = modifyNodeLabels(getElementAttribute(element, "labels"), labelOptions);
        result.properties = collectNodeOrEdgeProperties(reader, propertyKeyNameMap, "node");
        return result;
    }

    private String modifyNodeLabels(final String labels, final LabelOptions labelOptions) {
        final boolean prefixUsed = labelOptions.prefix != null && labelOptions.prefix.length() > 0;
        final boolean suffixUsed = labelOptions.suffix != null && labelOptions.suffix.length() > 0;
        if (!labelOptions.modifyNodeLabels || labels == null || labels.length() == 0 || (!prefixUsed && !suffixUsed))
            return labels;
        final String[] parts = StringUtils.split(labels, ':');
        final StringBuilder modifiedLabels = new StringBuilder();
        for (final String part : parts) {
            modifiedLabels.append(':');
            if (prefixUsed)
                modifiedLabels.append(labelOptions.prefix);
            modifiedLabels.append(part);
            if (suffixUsed)
                modifiedLabels.append(labelOptions.suffix);
        }
        return modifiedLabels.toString();
    }

    private Map<String, Object> collectNodeOrEdgeProperties(final XMLEventReader reader,
                                                            final Map<String, PropertyKey> propertyKeyNameMap,
                                                            final String forType) {
        final Map<String, Object> properties = new HashMap<>();
        while (reader.hasNext()) {
            final XMLEvent nextEvent = tryNextEvent(reader);
            if (nextEvent != null && nextEvent.isStartElement()) {
                final StartElement startChildElement = nextEvent.asStartElement();
                final String propertyKey = getElementAttribute(startChildElement, "key");
                final String forTypePropertyKey = forType + "|" + propertyKey;
                if (!propertyKeyNameMap.containsKey(forTypePropertyKey)) {
                    final PropertyKey property = new PropertyKey(propertyKey, forType, propertyKey, "string", null);
                    propertyKeyNameMap.put(forTypePropertyKey, property);
                    if (LOGGER.isInfoEnabled())
                        LOGGER.warn(forType + " property '" + propertyKey +
                                    "' wasn't defined, fallback to string property");
                }
                final PropertyKey property = propertyKeyNameMap.get(forTypePropertyKey);
                final String propertyName = property.attributeName;
                if (!propertyName.equals("labels") && !propertyName.equals("label"))
                    properties.put(propertyName, parsePropertyValue(property, reader));
            } else if (nextEvent != null && nextEvent.isEndElement()) {
                final String tagName = nextEvent.asEndElement().getName().getLocalPart();
                if (tagName.equalsIgnoreCase("node") || tagName.equalsIgnoreCase("edge"))
                    break;
            }
        }
        return properties;
    }

    private Object parsePropertyValue(final PropertyKey type, final XMLEventReader reader) {
        String value = tryGetElementText(reader);
        if (value == null)
            return null;
        if (type.attributeList != null) {
            value = StringUtils.strip(value, "[] \t\n\r");
            switch (type.attributeList.toLowerCase(Locale.US)) {
                case "boolean":
                    return Arrays.stream(StringUtils.split(value, ',')).map(x -> Boolean.valueOf(x.trim())).collect(
                            Collectors.toList());
                case "int":
                    return Arrays.stream(StringUtils.split(value, ',')).map(x -> Integer.valueOf(x.trim())).collect(
                            Collectors.toList());
                case "long":
                    return Arrays.stream(StringUtils.split(value, ',')).map(x -> Long.valueOf(x.trim())).collect(
                            Collectors.toList());
                case "float":
                    return Arrays.stream(StringUtils.split(value, ',')).map(x -> Float.valueOf(x.trim())).collect(
                            Collectors.toList());
                case "double":
                    return Arrays.stream(StringUtils.split(value, ',')).map(x -> Double.valueOf(x.trim())).collect(
                            Collectors.toList());
                case "string": {
                    boolean insideString = false;
                    int start = 0;
                    int escapeCount = 0;
                    List<String> parts = new ArrayList<>();
                    for (int i = 0; i < value.length(); i++) {
                        char currentChar = value.charAt(i);
                        if (currentChar == '"') {
                            if (insideString && escapeCount % 2 == 0) {
                                parts.add(value.substring(start, i).replace("\\\"", "\""));
                                insideString = false;
                            } else if (!insideString) {
                                insideString = true;
                                start = i + 1;
                            }
                        }
                        escapeCount = currentChar == '\\' ? escapeCount + 1 : 0;
                    }
                    return parts;
                }
            }
        } else {
            switch (type.attributeType.toLowerCase(Locale.US)) {
                case "boolean":
                    return Boolean.valueOf(value);
                case "int":
                    return Integer.valueOf(value);
                case "long":
                    return Long.valueOf(value);
                case "float":
                    return Float.valueOf(value);
                case "double":
                    return Double.valueOf(value);
                case "string":
                    return value;
            }
        }
        return value;
    }

    private String tryGetElementText(final XMLEventReader reader) {
        try {
            return reader.getElementText();
        } catch (XMLStreamException e) {
            LOGGER.warn("Failed to read XML element text", e);
            return null;
        }
    }

    private Version getNeo4jKernelVersion(final Session session) {
        final Transaction tx = session.beginTransaction();
        final Result result = tx.run(
                "call dbms.components() yield versions, edition unwind versions as version return version, edition");
        final Record record = result.single();
        tx.commit();
        final String version = record.get("version").asString();
        final String edition = record.get("edition").asString();
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Detected Neo4j database version " + version + " (" + edition + ")");
        return Version.tryParse(version);
    }

    private Map<String, Long> importAllNodes(final Session session, final Path inputFile,
                                             final LabelOptions labelOptions,
                                             final Map<String, PropertyKey> propertyKeyNameMap,
                                             final AtomicLong nodeCount) {
        final AtomicReference<Transaction> tx = new AtomicReference<>(session.beginTransaction());
        final Map<String, Long> nodeIdNeo4jIdMap = new HashMap<>();
        final AtomicLong counter = new AtomicLong();
        final Map<String, List<Node>> perLabelBatches = new HashMap<>();
        handleAllElementsInXMLWithTag(inputFile, "node", (reader, startElement) -> {
            final String labels = modifyNodeLabels(getElementAttribute(startElement, "labels"), labelOptions);
            if (!perLabelBatches.containsKey(labels))
                perLabelBatches.put(labels, new ArrayList<>());
            final List<Node> batch = perLabelBatches.get(labels);
            batch.add(parseNode(reader, startElement, propertyKeyNameMap, labelOptions));
            if (batch.size() >= BATCH_SIZE) {
                nodeIdNeo4jIdMap.putAll(runCreateNodeBatch(tx.get(), batch, labels));
                batch.clear();
            }
            final long currentCount = counter.incrementAndGet();
            if (currentCount % 5000 == 0)
                LOGGER.info("Nodes progress: " + currentCount + "/" + nodeCount.get());
            if (currentCount % TRANSACTION_SIZE == 0) {
                tx.get().commit();
                tx.set(session.beginTransaction());
            }
        });
        for (final String labels : perLabelBatches.keySet()) {
            final List<Node> batch = perLabelBatches.get(labels);
            if (batch.size() > 0)
                nodeIdNeo4jIdMap.putAll(runCreateNodeBatch(tx.get(), batch, labels));
        }
        tx.get().commit();
        return nodeIdNeo4jIdMap;
    }

    private Map<String, Long> runCreateNodeBatch(final Transaction tx, final List<Node> nodes, final String labels) {
        final Map<String, Object> batch = new HashMap<>();
        final List<Map<String, Object>> batchList = new ArrayList<>();
        batch.put("batch", batchList);
        for (final Node node : nodes) {
            final Map<String, Object> nodeMap = new HashMap<>();
            nodeMap.put("id", node.id);
            nodeMap.put("properties", node.properties);
            batchList.add(nodeMap);
        }
        final Map<String, Long> nodeIdNeo4jIdMap = new HashMap<>();
        final Result result = tx.run(
                "UNWIND $batch as row\nCREATE (n" + labels + ")\nSET n += row.properties\nRETURN row.id, id(n)", batch);
        result.stream().forEach(r -> nodeIdNeo4jIdMap.put(r.get(0).asString(), r.get(1).asLong()));
        return nodeIdNeo4jIdMap;
    }

    private void importAllEdges(final Session session, final Path inputFile, final LabelOptions labelOptions,
                                final Map<String, PropertyKey> propertyKeyNameMap, final AtomicLong edgeCount,
                                final Map<String, Long> nodeIdNeo4jIdMap) {
        final AtomicReference<Transaction> tx = new AtomicReference<>(session.beginTransaction());
        final AtomicLong counter = new AtomicLong();
        final Map<String, List<Edge>> perLabelBatches = new HashMap<>();
        handleAllElementsInXMLWithTag(inputFile, "edge", (reader, startElement) -> {
            final String edgeLabel = modifyEdgeLabel(getElementAttribute(startElement, "label"), labelOptions);
            if (!perLabelBatches.containsKey(edgeLabel))
                perLabelBatches.put(edgeLabel, new ArrayList<>());
            final List<Edge> batch = perLabelBatches.get(edgeLabel);
            batch.add(parseEdge(reader, startElement, propertyKeyNameMap, labelOptions));
            if (batch.size() >= BATCH_SIZE) {
                runCreateEdgeBatch(tx.get(), batch, edgeLabel, nodeIdNeo4jIdMap);
                batch.clear();
            }
            final long currentCount = counter.incrementAndGet();
            if (currentCount % 5000 == 0)
                LOGGER.info("Edges progress: " + currentCount + "/" + edgeCount.get());
            if (currentCount % TRANSACTION_SIZE == 0) {
                tx.get().commit();
                tx.set(session.beginTransaction());
            }
        });
        for (final String edgeLabel : perLabelBatches.keySet()) {
            final List<Edge> batch = perLabelBatches.get(edgeLabel);
            if (batch.size() > 0)
                runCreateEdgeBatch(tx.get(), batch, edgeLabel, nodeIdNeo4jIdMap);
        }
        tx.get().commit();
    }

    private String modifyEdgeLabel(final String label, final LabelOptions labelOptions) {
        final boolean prefixUsed = labelOptions.prefix != null && labelOptions.prefix.length() > 0;
        final boolean suffixUsed = labelOptions.suffix != null && labelOptions.suffix.length() > 0;
        if (!labelOptions.modifyEdgeLabels || label == null || label.length() == 0 || (!prefixUsed && !suffixUsed))
            return label;
        final StringBuilder modifiedLabels = new StringBuilder();
        if (prefixUsed)
            modifiedLabels.append(labelOptions.prefix);
        modifiedLabels.append(label);
        if (suffixUsed)
            modifiedLabels.append(labelOptions.suffix);
        return modifiedLabels.toString();
    }

    private Edge parseEdge(final XMLEventReader reader, final StartElement element,
                           final Map<String, PropertyKey> propertyKeyNameMap, final LabelOptions labelOptions) {
        Edge result = new Edge();
        result.label = modifyEdgeLabel(getElementAttribute(element, "label"), labelOptions);
        result.source = getElementAttribute(element, "source");
        result.target = getElementAttribute(element, "target");
        result.properties = collectNodeOrEdgeProperties(reader, propertyKeyNameMap, "edge");
        return result;
    }

    private void runCreateEdgeBatch(final Transaction tx, final List<Edge> edges, String label,
                                    final Map<String, Long> nodeIdNeo4jIdMap) {
        final Map<String, Object> batch = new HashMap<>();
        final List<Map<String, Object>> batchList = new ArrayList<>();
        batch.put("batch", batchList);
        for (final Edge edge : edges) {
            final Map<String, Object> nodeMap = new HashMap<>();
            nodeMap.put("source", nodeIdNeo4jIdMap.get(edge.source));
            nodeMap.put("target", nodeIdNeo4jIdMap.get(edge.target));
            nodeMap.put("properties", edge.properties);
            batchList.add(nodeMap);
        }
        if (label.startsWith(":"))
            label = label.substring(1);
        tx.run("UNWIND $batch as row\nMATCH (a),(b) WHERE id(a)=row.source AND id(b)=row.target\nCREATE (a)-[e:" +
               label + "]->(b)\nSET e += row.properties", batch);
    }

    private void createIndices(final Version neo4jVersion, final Session session,
                               final Map<String, List<String>> indices) {
        final boolean useNewIndexCreation = neo4jVersion != null && neo4jVersion.compareTo(
                NEW_INDEX_CREATION_NEO4J_VERSION) >= 0;
        final Map<String, Set<String>> existingIndices = useNewIndexCreation ? new HashMap<>() : getExistingIndices(
                session);
        final Transaction tx = session.beginTransaction();
        for (final String label : indices.keySet()) {
            final List<String> propertyKeys = indices.get(label);
            for (final String propertyKey : propertyKeys) {
                if (LOGGER.isInfoEnabled())
                    LOGGER.info("Create index on label '" + label + "' and property '" + propertyKey + "'");
                if (useNewIndexCreation)
                    tx.run("CREATE INDEX IF NOT EXISTS FOR (t:" + label + ") ON (t." + propertyKey + ")");
                else {
                    if (!existingIndices.containsKey(label) || !existingIndices.get(label).contains(propertyKey))
                        tx.run("CREATE INDEX ON :" + label + " (" + propertyKey + ")");
                    else if (LOGGER.isInfoEnabled())
                        LOGGER.info("Skipping index creation on :" + label + " (" + propertyKey +
                                    ") because a similar index already exists");
                }
            }
        }
        tx.commit();
    }

    private Map<String, Set<String>> getExistingIndices(final Session session) {
        final Map<String, Set<String>> indices = new HashMap<>();
        final Transaction tx = session.beginTransaction();
        final List<Record> queryResult = tx.run("CALL db.indexes").list();
        for (final Record index : queryResult) {
            final List<String> labels = index.get("tokenNames").asList(Value::asString);
            final List<String> propertyKeys = index.get("properties").asList(Value::asString);
            if (labels.size() > 1 && LOGGER.isWarnEnabled())
                LOGGER.warn("Found multiple labels for index " + index + ". Ignoring all but first label.");
            if (!indices.containsKey(labels.get(0)))
                indices.put(labels.get(0), new HashSet<>());
            indices.get(labels.get(0)).addAll(propertyKeys);
        }
        tx.commit();
        return indices;
    }

    private interface Callback<T, U> {
        void callback(T t, U u);
    }

    private static class LabelOptions {
        boolean modifyNodeLabels;
        boolean modifyEdgeLabels;
        String prefix;
        String suffix;
    }

    private abstract static class PropertyContainer {
        Map<String, Object> properties;
    }

    private static class Node extends PropertyContainer {
        String id;
        String labels;
    }

    private static class Edge extends PropertyContainer {
        String source;
        String target;
        String label;
    }
}
