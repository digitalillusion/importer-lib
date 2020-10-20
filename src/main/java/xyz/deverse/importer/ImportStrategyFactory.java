package xyz.deverse.importer;

public interface ImportStrategyFactory {

	ImportStrategy<?, ? extends ImportLine> createImportStrategy(Importer<?, ? extends ImportLine> importer);

}