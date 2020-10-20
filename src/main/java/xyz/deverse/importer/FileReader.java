package xyz.deverse.importer;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class FileReader<T, S extends ImportLine> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

    private final Class<S> lineType;

    protected final MultipartFile file;

    @Setter
    private Iterator<T> iterator;

    private final LinkedList<Consumer<ImportLine>> lineProcessors;

    protected List<List<String>> headers = new ArrayList<>();

    public FileReader(MultipartFile file, Class<S> lineType, LinkedList<Consumer<ImportLine>> lineProcessors, Collection<ImportLine> importedLines) {
        this.lineType = lineType;
        this.file = file;
        this.lineProcessors = lineProcessors;
    }

    public void abort() {
        if (iterator != null) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
    }

    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    public abstract Stream<ImportLine> read(ReadFilter filter);

    public void onParseLine(ImportLine parsedLine) {
        Iterator<Consumer<ImportLine>> iterator = lineProcessors.descendingIterator();
        iterator.forEachRemaining(processor -> {
            try {
                processor.accept(parsedLine);
            } catch (ClassCastException e) {
                LOGGER.debug("Processor " + processor.toString() + " does not handle the ImportLine subclass " + parsedLine.getClass().getCanonicalName() + " and will be skipped.");
            }
        });
    }

    public List<String> getHeadersByGroupIndex(int groupIndex) {
        return headers.get(groupIndex);
    }

    public abstract ReadFilter createFilter();
}
