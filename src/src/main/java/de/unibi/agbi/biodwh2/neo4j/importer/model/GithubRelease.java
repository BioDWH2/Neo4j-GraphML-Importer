package de.unibi.agbi.biodwh2.neo4j.importer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubRelease {
    @JsonProperty("url")
    public String url;
    @JsonProperty("html_url")
    public String htmlUrl;
    @JsonProperty("id")
    public Integer id;
    @JsonProperty("tag_name")
    public String tagName;
    @JsonProperty("assets")
    public List<GithubAsset> assets;
}
