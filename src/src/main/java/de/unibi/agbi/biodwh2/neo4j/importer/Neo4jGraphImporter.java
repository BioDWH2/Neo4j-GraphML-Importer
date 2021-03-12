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
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class Neo4jGraphImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphImporter.class);
    private static final String RELEASE_URL = "https://api.github.com/repos/BioDWH2/Neo4j-GraphML-Importer/releases";
    private static final int BATCH_SIZE = 1000;

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
                          commandLine.labelPrefix);
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

    private void printHelp(final CmdArgs commandLine) {
        CommandLine.usage(commandLine, System.out);
    }

    private void importGraphML(final String inputFilePath, final String endpoint, final String username,
                               final String password, final String labelPrefix) {
        if (!Paths.get(inputFilePath).toFile().exists()) {
            LOGGER.error("Input file '" + inputFilePath + "' not found");
            return;
        }
        final Path inputFile = Paths.get(inputFilePath);
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
        final Set<String> uniqueNodeLabels = new HashSet<>();
        handleAllElementsInXMLWithTag(inputFile, "node", (reader, startElement) -> uniqueNodeLabels
                .add(prefixLabels(getElementAttribute(startElement, "labels"), labelPrefix)));
        final Set<String> uniqueEdgeLabels = new HashSet<>();
        handleAllElementsInXMLWithTag(inputFile, "edge", (reader, startElement) -> uniqueEdgeLabels
                .add(prefixLabels(getElementAttribute(startElement, "label"), labelPrefix)));
        try (final Driver driver = GraphDatabase.driver(endpoint, getAuthToken(username, password))) {
            try (final Session session = driver.session()) {
                final Map<String, Long> nodeIdNeo4jIdMap = new HashMap<>();
                final AtomicLong counter = new AtomicLong();
                for (final String label : uniqueNodeLabels) {
                    final Transaction tx = session.beginTransaction();
                    List<Node> nodes = new ArrayList<>();
                    handleAllElementsInXMLWithTag(inputFile, "node", (reader, startElement) -> {
                        final String labels = prefixLabels(getElementAttribute(startElement, "labels"), labelPrefix);
                        if (!label.equals(labels))
                            return;
                        nodes.add(parseNode(reader, startElement, propertyKeyNameMap, labelPrefix));
                        final long currentCount = counter.incrementAndGet();
                        if (nodes.size() >= BATCH_SIZE) {
                            LOGGER.info("Batch create nodes progress: " + currentCount + "/" + nodeCount.get());
                            nodeIdNeo4jIdMap.putAll(runCreateNodeBatch(tx, nodes, label));
                            nodes.clear();
                        }
                    });
                    if (nodes.size() > 0)
                        nodeIdNeo4jIdMap.putAll(runCreateNodeBatch(tx, nodes, label));
                    tx.commit();
                }
                counter.set(0);
                for (final String label : uniqueEdgeLabels) {
                    final Transaction tx = session.beginTransaction();
                    List<Edge> edges = new ArrayList<>();
                    handleAllElementsInXMLWithTag(inputFile, "edge", (reader, startElement) -> {
                        final String edgeLabel = prefixLabels(getElementAttribute(startElement, "label"), labelPrefix);
                        if (!label.equals(edgeLabel))
                            return;
                        edges.add(parseEdge(reader, startElement, propertyKeyNameMap, labelPrefix));
                        final long currentCount = counter.incrementAndGet();
                        if (edges.size() >= BATCH_SIZE) {
                            LOGGER.info("Batch create edges progress: " + currentCount + "/" + edgeCount.get());
                            runCreateEdgeBatch(tx, edges, label, nodeIdNeo4jIdMap);
                            edges.clear();
                        }
                    });
                    if (edges.size() > 0)
                        runCreateEdgeBatch(tx, edges, label, nodeIdNeo4jIdMap);
                    tx.commit();
                }
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
                           final Map<String, PropertyKey> propertyKeyNameMap, final String labelPrefix) {
        Node result = new Node();
        result.id = getElementAttribute(element, "id");
        result.labels = prefixLabels(getElementAttribute(element, "labels"), labelPrefix);
        result.properties = collectNodeOrEdgeProperties(reader, propertyKeyNameMap, "node");
        return result;
    }

    private String prefixLabels(String labels, final String prefix) {
        if (prefix == null || labels.length() == 0)
            return labels;
        final String[] parts = StringUtils.split(labels, ':');
        final String delimiterAndPrefix = ':' + prefix;
        return delimiterAndPrefix + String.join(delimiterAndPrefix, parts);
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

    private Edge parseEdge(final XMLEventReader reader, final StartElement element,
                           final Map<String, PropertyKey> propertyKeyNameMap, final String labelPrefix) {
        Edge result = new Edge();
        result.label = prefixLabels(getElementAttribute(element, "label"), labelPrefix);
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

    private interface Callback<T, U> {
        void callback(T t, U u);
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
