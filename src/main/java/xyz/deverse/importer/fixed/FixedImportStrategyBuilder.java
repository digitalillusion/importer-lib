package xyz.deverse.importer.fixed;

import xyz.deverse.importer.ImportStrategy;
import xyz.deverse.importer.misc.ParameterizedClassTypeResolver;

import java.lang.reflect.Type;

/**
 * Morpho import strategy builder
 * @param <T> : node type
 * @param <S> : line type
 */
public abstract class FixedImportStrategyBuilder<T, S extends FixedFileReader.FixedLine<T>> {

    /**
     * Concrete implementation
     */
    public abstract class FixedImportStrategy extends ImportStrategy<T, S> {

        FixedImportStrategy(Class<T> nodeType, Class<S> lineType, PostProcessCondition postProcessCondition) {
            super();
            this.postProcessCondition = postProcessCondition;
            this.nodeType = nodeType;
            this.lineType = lineType;
        }

        @Override
        public void parse() {
            fileReader = new FixedFileReader<T, S>(FixedImportStrategy.this.file,
                    FixedImportStrategy.this.lineType,
                    FixedImportStrategy.this.lineProcessors,
                    FixedImportStrategy.this.importedLines,
                    FixedImportStrategyBuilder.this.rowMapper) {};
            super.parse();
        }
    }


    private Class<T> nodeType;

    private Class<S> lineType;

    private FixedFileReader.StringImportMapper<T, S> rowMapper;

    private ImportStrategy.PostProcessCondition postProcessCondition = ImportStrategy.PostProcessCondition.ON_ALL_LINES;

    public FixedImportStrategyBuilder() {
        Type[] typeParameters = ParameterizedClassTypeResolver.getTypeParameters(getClass());
        this.nodeType = (Class<T>) typeParameters[0];
        this.lineType = (Class<S>) typeParameters[1];
    }

    public FixedImportStrategyBuilder<T, S> withRowMapper(FixedFileReader.StringImportMapper<T, S> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public FixedImportStrategyBuilder<T, S> withPostProcessCondition(ImportStrategy.PostProcessCondition postProcessCondition) {
        this.postProcessCondition = postProcessCondition;
        return this;
    }

    public FixedImportStrategy build() {
        return new FixedImportStrategy(this.nodeType, this.lineType, this.postProcessCondition) {};
    }


}
