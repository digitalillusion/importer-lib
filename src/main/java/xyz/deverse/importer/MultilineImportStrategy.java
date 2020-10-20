package xyz.deverse.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import lombok.Getter;
import lombok.Setter;
import xyz.deverse.importer.csv.CsvFileReader;

/**
 * Import strategy that parses a collection of nodes
 */
public class MultilineImportStrategy<T> extends ImportStrategy<T, ImportLine> {
	boolean parsed;
	ActionType actionType;
	Iterator<ImportLine> rowIterator;
	Class<? extends ImportLine> lineType;
	

	@Getter
	@Setter
	private Collection<ImportLine> results;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MultilineImportStrategy.class);

	/**
	 * @param nodes The nodes to parse
	 * @param actionType The requested action type
	 */
	private MultilineImportStrategy(List<T> nodes, ActionType actionType, Class<? extends ImportLine> lineType) {
		this.rowIterator = nodes.stream().map(n -> makeLine(n, nodes.indexOf(n), actionType)).iterator();
		this.actionType = actionType;
		this.lineType = lineType;
		this.parsed = false;
	}
	
	/**
	 * @param lines The line to parse
	 * @param actionType The requested action type
	 */
	private MultilineImportStrategy(List<ImportLine> lines) {
		this.rowIterator = lines.iterator();
		this.actionType = lines.size() > 0 ? lines.get(0).getActionType() : ActionType.PERSIST;
		this.lineType = lines.size() > 0 ? lines.get(0).getClass() : ImportLine.class;
		this.parsed = false;
	}

	private ImportLine makeLine(T node, int index, ActionType actionType) {
		return ImportMapper.MappedLine.<T>builder()//
				.nodes(Collections.singletonList(node))//
				.actionType(actionType)//
				.count(1)//
				.index(index)//
				.indexInGroup(index)
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
		List<ImportLine> read = new ArrayList<>();
		while (rowIterator.hasNext()) {
			ImportLine next = rowIterator.next();
			try {
				if (ImportMapper.MappedLine.class.isAssignableFrom(lineType)) {
					ImportLine mappedLine = (ImportMapper.MappedLine<T>) next;
					lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(mappedLine));
				} else if (CsvFileReader.CsvLine.class.isAssignableFrom(lineType)) {
					CsvFileReader.CsvLine csvLine = (CsvFileReader.CsvLine) next;
					lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(csvLine));
				}
			} catch (Exception e) {
				LOGGER.error("Exception occurred in MultilineImportStrategy", e);
				ImportMapper.MappedLine<T> errorLine = ImportMapper.MappedLine.<T>builder()//
						.actionType(next.getActionType())//
						.message(e.getClass() + ": " + e.getMessage())
						.nodes(Collections.emptyList())
						.count(next.getCount())//
						.index(next.getIndex())//
						.indexInGroup(next.getIndexInGroup())
						.excludedIds(Collections.emptySet())
						.severity(Level.ERROR)//
						.saveDepth(new AtomicInteger(0))//
						.build();
				lineProcessors.descendingIterator().forEachRemaining(processor -> processor.accept(errorLine));
				read.add(errorLine);
			}
		}
		return read.stream();
	}

	/**
	 * Provide an {@code ImportStrategyFactory} implementation that will return a {@code MultilineImportStrategy} for the given input
	 * 
	 * @param nodes The node to parse
	 * @param actionType The action type requested
	 * @return ImportStrategyFactory
	 */
	public static <T> ImportStrategyFactory factory(List<T> nodes, ActionType actionType) {
		return importer -> {
			if (nodes.size() == 0 || Objects.equals(nodes.get(0).getClass(), importer.getNodeType()) && Objects.equals(ImportMapper.MappedLine.class, importer.getLineType())) {
				return new MultilineImportStrategy<>(nodes, actionType, importer.getLineType());
			} else {
				throw new UnsupportedOperationException("This ImportStrategyFactory does not support the importer " + importer.getClass().getCanonicalName());
			}
		};
	}
	
	/**
	 * Provide an {@code ImportStrategyFactory} implementation that will return a {@code MultilineImportStrategy} for the given input
	 * 
	 * @param line The line to parse
	 * @return ImportStrategyFactory
	 */
	public static ImportStrategyFactory factory(List<ImportLine> lines) {
		return importer -> {
			if (lines != null && CsvFileReader.CsvLine.class.isAssignableFrom(importer.getLineType())) {
				return new MultilineImportStrategy<>(lines);
			} else {
				throw new UnsupportedOperationException("This ImportStrategyFactory does not support the importer " + importer.getClass().getCanonicalName());
			}
		};
	}
}
