package xyz.deverse.importer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.CaseFormat;

import lombok.Builder;
import lombok.Value;
import xyz.deverse.importer.generic.ImportTag;

@Builder
@Value
public class ImporterProcessStatus {
	public static String getCacheId(List<? extends ImportTag> importTags) {
		List<String> res = new ArrayList<>();
		res.add(CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN).convert(ImporterProcessStatus.class.getSimpleName()));
		for (ImportTag importTag : importTags) {
			res.add(importTag.name());
		}
		return String.join("-", res);
	}

	boolean aborted;

	@Builder.Default
	Set<String> categories = new HashSet<>();

	boolean completed;

	List<? extends ImportTag> importTags;

	@Builder.Default
	List<ImportLine> results = new ArrayList<>();

	String selected;

	boolean started;

	Long timestamp;

	int linesSent;
	
	String filename;
	
	public static enum ImportStatus {
		ABORTED,
		COMPLETED,
		STARTED
	}

	@JsonIgnore
	public ImportStatus getImportStatus() {
		// Order is relevant
		if (isAborted()) {
			return ImportStatus.ABORTED;
		} else if (isCompleted()) {
			return ImportStatus.COMPLETED;
		} else 
		return ImportStatus.STARTED;
	}

	public ImporterProcessStatus withResultSubList(int from, int to) {
		return ImporterProcessStatus.builder()
				.categories(getCategories())
				.timestamp(getTimestamp())
				.completed(isCompleted())
				.importTags(getImportTags())
				.results(results.subList(from, to))
				.selected(getSelected())
				.started(isStarted())
				.aborted(isAborted())
				.filename(getFilename())
				.linesSent(getLinesSent())
				.build();
	}
	
	public ImporterProcessStatus withoutResultNodes() {
		return ImporterProcessStatus.builder()
				.categories(getCategories())
				.timestamp(getTimestamp())
				.completed(isCompleted())
				.importTags(getImportTags())
				.results(results.stream().map(result -> result.withoutNodes()).collect(Collectors.toList()))
				.selected(getSelected())
				.started(isStarted())
				.aborted(isAborted())
				.filename(getFilename())
				.linesSent(getLinesSent())
				.build();
	}
}
