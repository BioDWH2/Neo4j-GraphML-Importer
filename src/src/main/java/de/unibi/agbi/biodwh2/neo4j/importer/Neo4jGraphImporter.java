package de.unibi.agbi.biodwh2.neo4j.importer;

import com.ctc.wstx.exc.WstxEOFException;
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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
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
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class Neo4jGraphImporter {
    private static final Logger LOGGER = LogManager.getLogger(Neo4jGraphImporter.class);
    private static final String RELEASE_URL = "https://api.github.com/repos/BioDWH2/Neo4j-GraphML-Importer/releases";
    private static final int BATCH_SIZE = 1000;
    private static final Version NEO4J_4_VERSION = new Version(4, 0);
    private static final Version NEW_INDEX_CREATION_NEO4J_VERSION = new Version(4, 1, 3);
    private static final long TRANSACTION_SIZE = 20000;

    private Neo4jGraphImporter() {
    }

    public static void main(final String... args) {
        final CmdArgs commandLine = parseCommandLine(args);
        new Neo4jGraphImporter().run(commandLine);
    }

    private static CmdArgs parseCommandLine(final String... args) {
        final var result = new CmdArgs();
        final var cmd = new CommandLine(result);
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
            final var releaseUrl = new URL(RELEASE_URL);
            final List<GithubRelease> releases = mapper.readValue(releaseUrl, new TypeReference<>() {
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
            LOGGER.info("New version {} of Neo4j-GraphML-Importer is available at:", mostRecentVersion);
            LOGGER.info(mostRecentDownloadUrl);
            LOGGER.info("=======================================");
        }
    }

    private Version getCurrentVersion() {
        try {
            final Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                try {
                    final var manifest = new Manifest(resources.nextElement().openStream());
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
        final var result = new LabelOptions();
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
        final var indices = new HashMap<String, List<String>>();
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
                    LOGGER.warn(
                            "Failed to parse index '{}' will be ignored. Ensure the syntax <label1>.<property1>;<label2>.<property2>;...",
                            part);
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
        final Path inputFile = Paths.get(inputFilePath);
        if (!inputFile.toFile().exists()) {
            LOGGER.error("Input file '{}' not found", inputFilePath);
            return;
        }
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Parsing property definitions...");
        final var nodeCount = new AtomicLong();
        final var edgeCount = new AtomicLong();
        handleAllElementsInXMLWithTag(inputFile, "node", (reader, startElement) -> nodeCount.getAndIncrement());
        handleAllElementsInXMLWithTag(inputFile, "edge", (reader, startElement) -> edgeCount.getAndIncrement());
        LOGGER.info("{} nodes, {} edges", nodeCount.get(), edgeCount.get());
        final Map<String, PropertyKey> propertyKeyNameMap = new HashMap<>();
        handleAllElementsInXMLWithTag(inputFile, "key", (reader, startElement) -> {
            final PropertyKey property = getPropertyKeyFromElement(startElement);
            propertyKeyNameMap.put(property.forType() + "|" + property.id(), property);
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
        try (final var stream = openInputFile(inputFilePath)) {
            final XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(stream);
            while (reader.hasNext()) {
                final XMLEvent nextEvent = reader.nextEvent();
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

    private InputStream openInputFile(final Path inputFilePath) throws IOException {
        final var stream = new FileInputStream(inputFilePath.toFile());
        if (inputFilePath.toString().toLowerCase().endsWith(".gz")) {
            return new GZIPInputStream(stream);
        }
        return stream;
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
                           final Map<String, PropertyKey> propertyKeyNameMap,
                           final LabelOptions labelOptions) throws XMLStreamException {
        Node result = new Node();
        result.id = getElementAttribute(element, "id");
        result.labels = modifyNodeLabels(getElementAttribute(element, "labels"), labelOptions);
        result.properties = collectNodeOrEdgeProperties(reader, propertyKeyNameMap, "node");
        return result;
    }

    private String modifyNodeLabels(final String labels, final LabelOptions labelOptions) {
        final boolean prefixUsed = labelOptions.prefix != null && !labelOptions.prefix.isEmpty();
        final boolean suffixUsed = labelOptions.suffix != null && !labelOptions.suffix.isEmpty();
        if (!labelOptions.modifyNodeLabels || labels == null || labels.isEmpty() || (!prefixUsed && !suffixUsed))
            return labels;
        final String[] parts = StringUtils.split(labels, ':');
        final var modifiedLabels = new StringBuilder();
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
                                                            final String forType) throws XMLStreamException {
        final var properties = new HashMap<String, Object>();
        while (reader.hasNext()) {
            final XMLEvent nextEvent = tryNextEvent(reader);
            if (nextEvent == null)
                break;
            if (nextEvent.isStartElement()) {
                final StartElement startChildElement = nextEvent.asStartElement();
                final String propertyKey = getElementAttribute(startChildElement, "key");
                final String forTypePropertyKey = forType + "|" + propertyKey;
                if (!propertyKeyNameMap.containsKey(forTypePropertyKey)) {
                    final PropertyKey property = new PropertyKey(propertyKey, forType, propertyKey, "string", null);
                    propertyKeyNameMap.put(forTypePropertyKey, property);
                    if (LOGGER.isInfoEnabled())
                        LOGGER.warn("{} property '{}' wasn't defined, fallback to string property", forType,
                                    propertyKey);
                }
                final PropertyKey property = propertyKeyNameMap.get(forTypePropertyKey);
                final String propertyName = property.attributeName();
                if (!propertyName.equals("labels") && !propertyName.equals("label"))
                    properties.put(propertyName, parsePropertyValue(property, reader));
            } else if (nextEvent.isEndElement()) {
                final String tagName = nextEvent.asEndElement().getName().getLocalPart();
                if (tagName.equalsIgnoreCase("node") || tagName.equalsIgnoreCase("edge"))
                    break;
            }
        }
        return properties;
    }

    private XMLEvent tryNextEvent(final XMLEventReader reader) throws XMLStreamException {
        try {
            return reader.nextEvent();
        } catch (XMLStreamException e) {
            if (e instanceof WstxEOFException || e.getMessage().contains("Unexpected EOF"))
                throw e;
            LOGGER.warn("Failed to read XML event", e);
            return null;
        }
    }

    private Object parsePropertyValue(final PropertyKey type, final XMLEventReader reader) {
        String value = tryGetElementText(reader);
        if (value == null)
            return null;
        if (type.attributeList() != null) {
            return parsePropertyListValue(type, value);
        } else {
            return switch (type.attributeType().toLowerCase(Locale.US)) {
                case "boolean" -> Boolean.valueOf(value);
                case "int" -> Integer.valueOf(value);
                case "long" -> Long.valueOf(value);
                case "float" -> Float.valueOf(value);
                case "double" -> Double.valueOf(value);
                default -> value;
            };
        }
    }

    private Object parsePropertyListValue(final PropertyKey type, String value) {
        value = StringUtils.strip(value, "[] \t\n\r");
        return switch (type.attributeList().toLowerCase(Locale.US)) {
            case "boolean" -> convertStringToTypeList(value, Boolean::valueOf);
            case "int" -> convertStringToTypeList(value, Integer::valueOf);
            case "long" -> convertStringToTypeList(value, Long::valueOf);
            case "float" -> convertStringToTypeList(value, Float::valueOf);
            case "double" -> convertStringToTypeList(value, Double::valueOf);
            default -> {
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
                yield parts;
            }
        };
    }

    private <R> List<R> convertStringToTypeList(final String value, Function<String, R> mapper) {
        return Arrays.stream(StringUtils.split(value, ',')).map(String::strip).map(mapper).collect(Collectors.toList());
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
            LOGGER.info("Detected Neo4j database version {} ({})", version, edition);
        return Version.tryParse(version);
    }

    private Map<String, Long> importAllNodes(final Session session, final Path inputFile,
                                             final LabelOptions labelOptions,
                                             final Map<String, PropertyKey> propertyKeyNameMap,
                                             final AtomicLong nodeCount) {
        final var tx = new AtomicReference<>(session.beginTransaction());
        final var nodeIdNeo4jIdMap = new HashMap<String, Long>();
        final var counter = new AtomicLong();
        final var perLabelBatches = new HashMap<String, List<Node>>();
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
                LOGGER.info("Nodes progress: {}/{}", currentCount, nodeCount.get());
            if (currentCount % TRANSACTION_SIZE == 0) {
                tx.get().commit();
                tx.set(session.beginTransaction());
            }
        });
        for (final String labels : perLabelBatches.keySet()) {
            final List<Node> batch = perLabelBatches.get(labels);
            if (!batch.isEmpty())
                nodeIdNeo4jIdMap.putAll(runCreateNodeBatch(tx.get(), batch, labels));
        }
        tx.get().commit();
        return nodeIdNeo4jIdMap;
    }

    private Map<String, Long> runCreateNodeBatch(final Transaction tx, final List<Node> nodes, final String labels) {
        final var batch = new HashMap<String, Object>();
        final var batchList = new ArrayList<Map<String, Object>>();
        batch.put("batch", batchList);
        for (final Node node : nodes) {
            final var nodeMap = new HashMap<String, Object>();
            nodeMap.put("id", node.id);
            nodeMap.put("properties", node.properties);
            batchList.add(nodeMap);
        }
        final var nodeIdNeo4jIdMap = new HashMap<String, Long>();
        final Result result = tx.run(
                "UNWIND $batch as row\nCREATE (n" + labels + ")\nSET n += row.properties\nRETURN row.id, id(n)", batch);
        result.stream().forEach(r -> nodeIdNeo4jIdMap.put(r.get(0).asString(), r.get(1).asLong()));
        return nodeIdNeo4jIdMap;
    }

    private void importAllEdges(final Session session, final Path inputFile, final LabelOptions labelOptions,
                                final Map<String, PropertyKey> propertyKeyNameMap, final AtomicLong edgeCount,
                                final Map<String, Long> nodeIdNeo4jIdMap) {
        final var tx = new AtomicReference<>(session.beginTransaction());
        final var counter = new AtomicLong();
        final var perLabelBatches = new HashMap<String, List<Edge>>();
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
                LOGGER.info("Edges progress: {}/{}", currentCount, edgeCount.get());
            if (currentCount % TRANSACTION_SIZE == 0) {
                tx.get().commit();
                tx.set(session.beginTransaction());
            }
        });
        for (final String edgeLabel : perLabelBatches.keySet()) {
            final List<Edge> batch = perLabelBatches.get(edgeLabel);
            if (!batch.isEmpty())
                runCreateEdgeBatch(tx.get(), batch, edgeLabel, nodeIdNeo4jIdMap);
        }
        tx.get().commit();
    }

    private String modifyEdgeLabel(final String label, final LabelOptions labelOptions) {
        final boolean prefixUsed = labelOptions.prefix != null && !labelOptions.prefix.isEmpty();
        final boolean suffixUsed = labelOptions.suffix != null && !labelOptions.suffix.isEmpty();
        if (!labelOptions.modifyEdgeLabels || label == null || label.isEmpty() || (!prefixUsed && !suffixUsed))
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
                           final Map<String, PropertyKey> propertyKeyNameMap,
                           final LabelOptions labelOptions) throws XMLStreamException {
        final var result = new Edge();
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
                neo4jVersion, session);
        final Transaction tx = session.beginTransaction();
        for (final String label : indices.keySet()) {
            final List<String> propertyKeys = indices.get(label);
            for (final String propertyKey : propertyKeys) {
                if (LOGGER.isInfoEnabled())
                    LOGGER.info("Create index on label '{}' and property '{}'", label, propertyKey);
                if (useNewIndexCreation)
                    tx.run("CREATE INDEX IF NOT EXISTS FOR (t:" + label + ") ON (t." + propertyKey + ")");
                else {
                    if (!existingIndices.containsKey(label) || !existingIndices.get(label).contains(propertyKey))
                        tx.run("CREATE INDEX ON :" + label + " (" + propertyKey + ")");
                    else if (LOGGER.isInfoEnabled())
                        LOGGER.info("Skipping index creation on :{} ({}) because a similar index already exists", label,
                                    propertyKey);
                }
            }
        }
        tx.commit();
    }

    private Map<String, Set<String>> getExistingIndices(final Version neo4jVersion, final Session session) {
        final var indices = new HashMap<String, Set<String>>();
        final Transaction tx = session.beginTransaction();
        final List<Record> queryResult = tx.run("CALL db.indexes").list();
        for (final Record index : queryResult) {
            final String labelsKey = NEO4J_4_VERSION.compareTo(neo4jVersion) <= 0 ? "labelsOrTypes" : "tokenNames";
            final List<String> labels = index.get(labelsKey).asList(Value::asString);
            final List<String> propertyKeys = index.get("properties").asList(Value::asString);
            if (labels.size() > 1 && LOGGER.isWarnEnabled())
                LOGGER.warn("Found multiple labels for index {}. Ignoring all but first label.", index);
            if (!indices.containsKey(labels.get(0)))
                indices.put(labels.get(0), new HashSet<>());
            indices.get(labels.get(0)).addAll(propertyKeys);
        }
        tx.commit();
        return indices;
    }

    private interface Callback<T, U> {
        void callback(T t, U u) throws XMLStreamException;
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
