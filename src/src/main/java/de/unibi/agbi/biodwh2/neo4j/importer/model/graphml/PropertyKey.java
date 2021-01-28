package de.unibi.agbi.biodwh2.neo4j.importer.model.graphml;

public final class PropertyKey {
    public final String id;
    public final String forType;
    public final String attributeName;
    public final String attributeType;
    public final String attributeList;

    public PropertyKey(final String id, final String forType, final String attributeName, final String attributeType,
                       final String attributeList) {
        this.id = id;
        this.forType = forType;
        this.attributeName = attributeName;
        this.attributeType = attributeType;
        this.attributeList = attributeList;
    }
}
