package xyz.deverse.importer.fixed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import xyz.deverse.importer.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.web.multipart.MultipartFile;

import lombok.Getter;
import lombok.Setter;

/**
 * Simple file reader that read line per line file
 *
 * @param <S>
 */
public abstract class FixedFileReader<T, S extends FixedFileReader.FixedLine<T>> extends FileReader<String, S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedFileReader.class);

    public static interface StringImportMapper<T, S extends ImportLine> extends ImportMapper<T, S> {

        default BiFunction<S, String, S> propertyMapperFunction() {
            return (o, row) -> {
                Stream.of(o.getClass().getDeclaredFields())
                        .map(field -> {
                            field.setAccessible(true);
                            return field;
                        })
                        .filter(f -> f.isAnnotationPresent(FixedOffset.class))
                        .forEach(f -> {
                            try {
                                f.set(o, StringUtils.substring(row,
                                        f.getAnnotation(FixedOffset.class).start(),
                                        f.getAnnotation(FixedOffset.class).end()));
                            } catch (IllegalAccessException e) {
                                LOGGER.error("...");
                            }
                        });
                return o;
            };
        }

    }

    @Getter
    @Setter
    public static class FixedLine<T> implements ImportLine {
        int count;
        ActionType actionType = ActionType.PERSIST;
        int index;
        int indexInGroup;
        List<T> nodes = new ArrayList<>();
        Set<Long> excludedIds = new HashSet<>();
        String message;
        String group;
        List<String> groups;
        AtomicInteger saveDepth = new AtomicInteger(0);
        Level severity;

        @Override
        public List<T> getNodes() {
            return Optional.ofNullable(nodes).orElseGet(ArrayList::new);
        }

        @Override
        public Set<Long> getExcludedIds() {
            return Optional.ofNullable(excludedIds).orElseGet(HashSet::new);
        }

        @Override
        public ActionType getActionType() {
            return actionType;
        }

        @Override
        public ImportLine withoutNodes() {
            FixedLine<T> line = new FixedLine<T>();
            line.setActionType(getActionType());
            line.setGroup(getGroup());
            line.setCount(getCount());
            line.setExcludedIds(getExcludedIds());
            line.setIndex(getIndex());
            line.setIndexInGroup(getIndexInGroup());
            line.setMessage(getMessage());
            line.setNodes(new ArrayList<>());
            line.setSaveDepth(getSaveDepth());
            line.setSeverity(getSeverity());
            return line;
        }
    }



    private final StringImportMapper<T, S> mapper;

    private final Function<T, String> lineMessageFunction = node -> String.format("Imported instance %s", node.getClass().getSimpleName());

    public FixedFileReader(MultipartFile file, Class<S> lineType, LinkedList<Consumer<ImportLine>> lineProcessors, Collection<ImportLine> importedLines, StringImportMapper<T, S> mapper) {
        super(file, lineType, lineProcessors, importedLines);
        this.mapper = mapper;
    }
    
    private AtomicInteger index;

    @Override
    public ReadFilter createFilter() {
        return new ReadFilter();
    }

    @Override
    public Stream<ImportLine> read(ReadFilter filter) {
        BufferedReader reader;
        try {
            reader = initializeReader(getFile().getInputStream());
            LineNumberReader lineNumberReader = new LineNumberReader(reader);
            lineNumberReader.skip(Long.MAX_VALUE);
            int linesCount = lineNumberReader.getLineNumber();
            lineNumberReader.close();

            reader = initializeReader(getFile().getInputStream());
            setIterator(reader.lines().iterator());
            return StreamSupport //
                    .stream(Spliterators.spliteratorUnknownSize(getIterator(), Spliterator.ORDERED), false)
                    .map(row -> parseRow(getLineType(), mapper, index.getAndAdd(1), row)) //
                    .map(line -> {
                        try {
                            this.onParseLine(line);
                            if (!line.getNodes().isEmpty()) {
                                return ImportMapper.MappedLine.<T> builder()//
                                        .actionType(line.getActionType())
                                        .count(linesCount)
                                        .excludedIds(new HashSet<>())
                                        .group(line.getGroup())
                                        .index(index.get())
                                        .indexInGroup(index.get())
                                        .message(line.getNodes().stream().map(t -> lineMessageFunction.apply((T) t)).collect(Collectors.joining("\n")))//
                                        .nodes((List<T>) line.getNodes())//
                                        .severity(line.getSeverity())//
                                        .saveDepth(line.getSaveDepth())//
                                        .build();
                            }
                            return line;
                        } catch (RuntimeException e) {
                            LOGGER.error(e.getMessage(), e);
                            ImportLine errorLine = makeErrorLine(getLineType(), Level.ERROR, e, index.get(), index.get(), linesCount);
                            this.onParseLine(errorLine);
                            return errorLine;
                        }
                    });
        } catch (IOException ioe) {
            ImportLine[] errorLines = new ImportLine[] { makeErrorLine(getLineType(), Level.ERROR, ioe) };
            return StreamSupport //
                    .<ImportLine> stream(Spliterators.spliterator(errorLines, Spliterator.ORDERED), false) //
                    .peek(line -> this.onParseLine(line));
        }
    }

    private ImportLine parseRow(Class<S> clazz, StringImportMapper<T, S> mapper, Integer idx, String row) {
        // skipping header rows
        if (idx < mapper.skipLines()) {
            return parseRowForHeadersLine(clazz, idx, () -> "Skipping header line");
        // processing row
        } else {
            return parseRowForNodeLine(clazz, mapper, idx, row);
        }
    }

    protected ImportLine parseRowForNodeLine(Class<S> clazz, StringImportMapper<T, S> mapper, Integer idx, String row) {
        try {
            S fixedLine = clazz.newInstance();
            fixedLine.setSeverity(Level.INFO);
            fixedLine.setNodes(new ArrayList<>());
            fixedLine.setIndexInGroup(idx);
            fixedLine.setIndex(idx);
            fixedLine.setCount(idx);
            fixedLine.setSaveDepth(new AtomicInteger(0));
            mapper.propertyMapperFunction().apply(fixedLine, row);
            fixedLine.setNodes(Collections.singletonList(mapper.toNode(fixedLine)));
            if (mapper.isNeeded()) {
                return ImportMapper.MappedLine.<T> builder()//
                        .actionType(fixedLine.getActionType())
                        .count(fixedLine.getCount())
                        .excludedIds(new HashSet<>())
                        .group(fixedLine.getGroup())
                        .index(fixedLine.getIndex())
                        .indexInGroup(fixedLine.getIndexInGroup())
                        .message(fixedLine.getNodes().stream().map(t -> lineMessageFunction.apply((T) t)).collect(Collectors.joining("\n")))//
                        .nodes(fixedLine.getNodes())//
                        .severity(fixedLine.getSeverity())//
                        .saveDepth(fixedLine.getSaveDepth())//
                        .build();
            } else {
                return fixedLine;
            }
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    private S parseRowForHeadersLine(Class<S> clazz, Integer row, Supplier<String> headerMessageSupplier) {
        try {
            S fixedLine = clazz.newInstance();
            fixedLine.count = row;
            fixedLine.index = row;
            fixedLine.indexInGroup = row + 1;
            fixedLine.message = headerMessageSupplier.get();
            fixedLine.severity = Level.INFO;
            fixedLine.saveDepth = new AtomicInteger(0);
            return fixedLine;
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    private ImportLine makeErrorLine(Class<S> clazz, Level logLevel, Exception e) {
        return makeErrorLine(clazz, logLevel, e, 0, 0, 1);
    }

    private ImportLine makeErrorLine(Class<S> clazz, Level logLevel, Exception e, int index, int indexInGroup, int count) {
        try {
            FixedLine<T> errorLine = clazz.newInstance();
            errorLine.setCount(count);
            errorLine.setIndex(index);
            errorLine.setIndexInGroup(indexInGroup);
            errorLine.setMessage(e.getClass().getCanonicalName() + " " + e.getMessage());
            errorLine.setSeverity(logLevel);
            return errorLine;
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }

    private BufferedReader initializeReader(InputStream inputStream) {
    	index = new AtomicInteger();
        return new BufferedReader(new InputStreamReader(inputStream));
    }
}
