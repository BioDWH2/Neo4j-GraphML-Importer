package de.unibi.agbi.biodwh2.neo4j.importer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubAsset {
    @JsonProperty("url")
    public String url;
    @JsonProperty("browser_download_url")
    public String browserDownloadUrl;
    @JsonProperty("id")
    public Integer id;
    @JsonProperty("name")
    public String name;
    @JsonProperty("content_type")
    public String contentType;
}
