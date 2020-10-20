package xyz.deverse.importer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.event.Level;

import lombok.Builder;
import lombok.Value;
import xyz.deverse.importer.csv.CsvFileReader;
import xyz.deverse.importer.fixed.FixedFileReader;

public interface ImportMapper<T, S extends ImportLine> {

	/**
	 * @return Count of lines to expect as headers for {@link FileReader#read()}
	 */
	default int skipLines() {
		return 1;
	};

	/**
	 * @return True if the mapper is used to map each {@link ImportLine} toward a {@link ImportMapper.MappedLine}, false otherwise, in which case
	 * the {@link ImportLine} is returned on {@link FileReader#onParseLine(ImportLine)}
	 *
	 * Valid {@link ImportLine} can be one of those :
	 * - {@link CsvFileReader.CsvLine}
	 * - {@link FixedFileReader.FixedLine}
	 */
	default boolean isNeeded() {
		return true;
	}

	@Value
	@Builder
	class MappedLine<T> implements ImportLine {
		int count;
		String group;
		List<String> groups;
		int index;
		int indexInGroup;
		String message;
		List<T> nodes;
		Set<Long> excludedIds;
		Level severity;
		ActionType actionType;
		AtomicInteger saveDepth;

		@Override
		public ActionType getActionType() {
			return actionType == null? ActionType.PERSIST: actionType;
		}
		
		@Override
		public ImportLine withoutNodes() {
			return MappedLine.<T>builder()
					.count(count)
					.group(group)
					.index(index)
					.indexInGroup(indexInGroup)
					.message(message)
					.nodes(new ArrayList<>())
					.excludedIds(new HashSet<>())
					.severity(severity)
					.actionType(ActionType.PERSIST)
					.saveDepth(saveDepth)
					.build();
		}
	}

	T toNode(S row);
}