package xyz.deverse.importer;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import xyz.deverse.importer.csv.CsvFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import lombok.Getter;
import lombok.Setter;

/**
 * Import strategy that parses a collection of nodes
 */
public class SimpleImportStrategy<T> extends ImportStrategy<T, ImportLine> {
	boolean parsed;
	ActionType actionType;
	Iterator<ImportLine> rowIterator;
	List<T> nodes;
	Class<? extends ImportLine> lineType;
	

	@Getter
	@Setter
	private Collection<ImportLine> results;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleImportStrategy.class);

	/**
	 * @param nodes The nodes to parse
	 * @param actionType The requested action type
	 */
	private SimpleImportStrategy(List<T> nodes, ActionType actionType, Class<? extends ImportLine> lineType) {
		this.nodes = nodes;
		this.rowIterator = Collections.singletonList(makeLine(nodes, actionType)).iterator();
		this.actionType = actionType;
		this.lineType = lineType;
		this.parsed = false;
	}
	
	/**
	 * @param line The line to parse
	 * @param actionType The requested action type
	 */
	private SimpleImportStrategy(ImportLine line) {
		this.rowIterator = Collections.singletonList(line).iterator();
		this.actionType = line.getActionType();
		this.lineType = line.getClass();
		this.parsed = false;
	}

	private ImportLine makeLine(List<T> nodes, ActionType actionType) {
		return ImportMapper.MappedLine.<T>builder()//
				.nodes(nodes)//
				.actionType(actionType)//
				.count(1)//
				.index(0)//
				.severity(Level.INFO)//
				.excludedIds(Collections.emptySet())
				.saveDepth(new AtomicInteger(0))//
				.build();
	}

	@Override
	public void abort() {
		while (rowIterator.hasNext()) {
			rowIterator.next();
		}
	}

	@Override
	public boolean hasNext() {
		return rowIterator.hasNext();
	}

	@Override
	public Stream<ImportLine> read() {
		ImportLine next = rowIterator.next();
		try {
			if (ImportMapper.MappedLine.class.isAssignableFrom(lineType)) {
				ImportLine mappedLine = (ImportMapper.MappedLine<T>) next;
				lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(mappedLine));
				return Collections.singleton(mappedLine).stream();
			} else if (CsvFileReader.CsvLine.class.isAssignableFrom(lineType)) {
				ImportLine csvLine = (CsvFileReader.CsvLine) next;
				lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(csvLine));
				return Collections.singleton(csvLine).stream();
			}
		} catch (Exception e) {
			if (ImportMapper.MappedLine.class.isAssignableFrom(lineType)) {
				LOGGER.error("Exception occurred in SimpleImportStrategy", e);
				ImportLine errorLine = ImportMapper.MappedLine.<T>builder()//
						.actionType(next.getActionType())//
						.message(e.getClass() + ": " + e.getMessage())
						.nodes(Collections.emptyList())
						.count(next.getCount())//
						.index(0)//
						.indexInGroup(0)
						.severity(Level.ERROR)//
						.excludedIds(Collections.emptySet())
						.saveDepth(new AtomicInteger(0))//
						.build();
				lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(errorLine));
				return Collections.singleton(errorLine).stream();
			}
			throw e;
		} 
		return Collections.<ImportLine>emptySet().stream();
	}

	/**
	 * Provide an {@code ImportStrategyFactory} implementation that will return a {@code SimpleImportStrategy} for the given input
	 * 
	 * @param nodes The node to parse
	 * @param actionType The action type requested
	 * @return ImportStrategyFactory
	 */
	public static <T> ImportStrategyFactory factory(List<T> nodes, ActionType actionType) {
		return importer -> {
			if (nodes.size() == 0 || Objects.equals(nodes.get(0).getClass(), importer.getNodeType()) && Objects.equals(ImportMapper.MappedLine.class, importer.getLineType())) {
				return new SimpleImportStrategy<>(nodes, actionType, importer.getLineType());
			} else {
				throw new UnsupportedOperationException("This ImportStrategyFactory does not support the importer " + importer.getClass().getCanonicalName());
			}
		};
	}
	
	/**
	 * Provide an {@code ImportStrategyFactory} implementation that will return a {@code SimpleImportStrategy} for the given input
	 * 
	 * @param line The line to parse
	 * @return ImportStrategyFactory
	 */
	public static ImportStrategyFactory factory(ImportLine line) {
		return importer -> {
			if (line != null && CsvFileReader.CsvLine.class.isAssignableFrom(importer.getLineType())) {
				return new SimpleImportStrategy<>(line);
			} else {
				throw new UnsupportedOperationException("This ImportStrategyFactory does not support the importer " + importer.getClass().getCanonicalName());
			}
		};
	}
}
