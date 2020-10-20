package xyz.deverse.importer;

import xyz.deverse.importer.generic.ImportTag;

import java.util.Collection;
import java.util.List;

public interface Importer<T, S extends ImportLine> {

	List<? extends ImportTag> getImportTags();

	String getPublisherUrl();

	Class<T> getNodeType();

	Class<? extends ImportLine> getLineType();

	void onParseLine(S parsedLine);

	void process(ImportStrategy<T, S> strategy);

	default void preProcess() {
	};

	default void postProcess(Collection<ImportMapper.MappedLine<T>> lines) {
	};

	Boolean isMatching(List<? extends ImportTag> importTags);

}
