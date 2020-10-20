package xyz.deverse.importer;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ReadFilter {
    private Integer version;
    private String filename;
    private List<String> groups;
    private Map<String, Map<Integer, Iterable<String>>> rawData;
}
