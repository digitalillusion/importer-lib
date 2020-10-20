package xyz.deverse.importer.csv;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import xyz.deverse.importer.ReadFilter;
import xyz.deverse.importer.misc.ParameterizedClassTypeResolver;
import xyz.deverse.importer.ImportStrategy;
import org.springframework.core.convert.ConversionService;
import org.springframework.web.multipart.MultipartFile;

public abstract class CsvImportStrategyBuilder<T, S extends CsvFileReader.CsvLine<T>> {

    public abstract class CsvImportStrategy extends ImportStrategy<T, S> {

        ConversionService conversionService;

        Function<Integer,CsvFileReader.CsvImportMapper<T, S>> rowMapper;

        Consumer<MultipartFile> fileMetadataValidator;
        
        protected CsvImportStrategy(Class<T> nodeType, Class<S> lineType, Function<Integer,CsvFileReader.CsvImportMapper<T, S>> rowMapper, ConversionService conversionService, Consumer<MultipartFile> fileMetadataValidator, PostProcessCondition postProcessCondition, UnaryOperator<ReadFilter> filterModifier) {
            super();
            this.lineType = lineType;
            this.nodeType = nodeType;
            this.rowMapper = rowMapper;
            this.conversionService = conversionService;
            this.fileMetadataValidator = fileMetadataValidator;
            this.postProcessCondition = postProcessCondition;
            this.filterModifier = filterModifier;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void parse() {
            CsvFileReader<T, S> csvFileReader = initializeReader(file);
            csvFileReader.setConversionService(conversionService);
            csvFileReader.setLineMessageFunction(node -> node.getClass().getSimpleName());
            csvFileReader.setFileMetadataValidator(Optional.ofNullable(fileMetadataValidator));
            csvFileReader.setEmptyCellValueEvaluatorFunction(cellValue -> {
                String trimmedCellValue = cellValue.trim();
                return trimmedCellValue.isEmpty() || "-".equals(trimmedCellValue);
            });

            fileReader = csvFileReader;
            super.parse();
        }

        public CsvFileReader<T, S> initializeReader(MultipartFile file) {
            return new CsvFileReader<T, S>(file,
                    CsvImportStrategy.this.lineType,
                    CsvImportStrategy.this.lineProcessors,
                    CsvImportStrategy.this.importedLines,
                    CsvImportStrategy.this.rowMapper) {};
        }

    }

    private ConversionService conversionService;

    private Function<Integer,CsvFileReader.CsvImportMapper<T, S>> rowMapper;

    private Consumer<MultipartFile> fileMetadataValidator;

    private Class<S> lineType;

    private Class<T> nodeType;

    private ImportStrategy.PostProcessCondition postProcessCondition = ImportStrategy.PostProcessCondition.ON_EACH_LINE;

    private UnaryOperator<ReadFilter> filterModifier;

    @SuppressWarnings("unchecked")
    public CsvImportStrategyBuilder() {
        Type[] typeParameters = ParameterizedClassTypeResolver.getTypeParameters(getClass());
        this.nodeType = (Class<T>) typeParameters[0];
        this.lineType = (Class<S>) typeParameters[1];
    }

    public CsvImportStrategyBuilder<T, S> withConversionService(ConversionService conversionService) {
        this.conversionService = conversionService;
        return this;
    }

    public CsvImportStrategyBuilder<T, S> withRowMapper(Function<Integer,CsvFileReader.CsvImportMapper<T, S>> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public CsvImportStrategyBuilder<T, S> withMetadataValidation(Consumer<MultipartFile> fileMetadataValidator) {
        this.fileMetadataValidator = fileMetadataValidator;
        return this;
    }

    public CsvImportStrategyBuilder<T, S> withPostProcessCondition(ImportStrategy.PostProcessCondition postProcessCondition) {
        this.postProcessCondition = postProcessCondition;
        return this;
    }

    public CsvImportStrategyBuilder<T, S> withFilterModifier(UnaryOperator<ReadFilter> filterModifier) {
        this.filterModifier = filterModifier;
        return this;
    }

    public CsvImportStrategy build() {
        return new CsvImportStrategy(this.nodeType, this.lineType, this.rowMapper, this.conversionService, this.fileMetadataValidator, this.postProcessCondition, this.filterModifier) {
        };
    }
}