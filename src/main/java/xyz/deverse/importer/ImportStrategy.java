package xyz.deverse.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.web.multipart.MultipartFile;

import lombok.Getter;
import lombok.Setter;

/**
 * Interface used to abstract various import methods: CSV file, database, etc.
 *
 */
@Getter
public abstract class ImportStrategy<T, S extends ImportLine> {

	public enum PostProcessCondition {
		ON_EACH_LINE,
		ON_ALL_LINES
	}

	protected Class<T> nodeType;

	protected Class<S> lineType;
	/**
	 * The list of line processors. They are executed in <em>reverse</em> order (last is executed first) and the execution
	 * of each may affect the line itself so that the changes are then made available to the following processors
	 */
	protected LinkedList<Consumer<ImportLine>> lineProcessors;

	protected PostProcessCondition postProcessCondition;

	protected FileReader<?, S> fileReader;

	@Setter
	private Collection<S> results;

	protected Collection<ImportLine> importedLines;

	protected MultipartFile file;
	
	@Getter
	protected boolean aborted;

	@Setter
	protected UnaryOperator<ReadFilter> filterModifier;

	/**
	 * The final post processors, applied to the collected results
	 */
	@Getter
	protected Collection<Consumer<Collection<ImportMapper.MappedLine<T>>>> postProcessors;

	protected ImportStrategy() {
		clearProcessors();
		postProcessCondition = PostProcessCondition.ON_EACH_LINE;
	}

	public void clearProcessors() {
		this.lineProcessors = new LinkedList<>();
		this.postProcessors = new LinkedList<>();
		this.importedLines = new LinkedList<>();
	}

	public void abort() {
		aborted = true;
		fileReader.abort();
	}

	public boolean hasNext() {
		return fileReader == null ? false : fileReader.hasNext();
	}

	public void parse() {
		Stream<ImportLine> inputStream = read();

		Collection<?> collect = inputStream.collect(Collectors.toCollection(LinkedList::new))
				.stream().filter(result -> ImportMapper.MappedLine.class.isAssignableFrom(result.getClass())).collect(Collectors.toCollection(LinkedList::new));

		setResults((Collection<S>) collect);
		postProcessors.forEach(pp -> pp.accept( (Collection<ImportMapper.MappedLine<T>>) collect));
	}

	Stream<ImportLine> read() {
		ReadFilter filter = fileReader.createFilter();
		if (filterModifier != null) {
			return fileReader.read(filterModifier.apply(filter));
		}
		return fileReader.read(filter);
	}

	/**
	 * Reset the state of the strategy in order to start a new import
	 * @param file The CSV file to import
	 */
	public void reinitialize(MultipartFile file) {
		clearProcessors();
		setResults(new ArrayList<>());
		this.getImportedLines().clear();
		this.file = file;
		this.aborted = false;
	}
}
