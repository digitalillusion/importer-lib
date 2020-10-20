package xyz.deverse.importer.importer.csv;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.web.multipart.MultipartFile;

import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.deverse.importer.ImportLine;
import xyz.deverse.importer.ImportStrategy;
import xyz.deverse.importer.Importer;
import xyz.deverse.importer.ReadFilter;
import xyz.deverse.importer.csv.CsvFileReader;
import xyz.deverse.importer.csv.CsvImportStrategyBuilder;

class GenericTestInstance {

}

public class TestCsvImportStrategy {

	@Autowired
	ConversionService conversionService;

	@Data
	@EqualsAndHashCode(callSuper=false)
	public static class GenericTestCsvLine extends CsvFileReader.CsvLine<GenericTestInstance> {

		String cell0;

		String cell1;

		Integer cell2;
	}

	CsvFileReader<GenericTestInstance, GenericTestCsvLine> csvFileReader;

	CsvImportStrategyBuilder<?,?>.CsvImportStrategy csvImport;

	MultipartFile file;

	Importer<GenericTestInstance, ImportLine> importer;

	boolean parsedLines;

	@Before
	public void setup() {
		parsedLines = false;
		csvFileReader = mock(CsvFileReader.class);
		file = mock(MultipartFile.class);
		importer = mock(Importer.class);

		when(csvFileReader.createFilter()).thenReturn(new ReadFilter());

		when(csvFileReader.read(any(ReadFilter.class))).then(invocation -> {
			this.importer.onParseLine(null);
			return new ArrayList<>().stream();
		});

		when(csvFileReader.hasNext()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
	}

	@Test
	public void testHasNext() throws ClassNotFoundException {
		csvImport = instanceStrategy();

		assertEquals("When import has not starter hasNext should be false", false, csvImport.hasNext());
		csvImport.parse();

		assertEquals("Wrong result for hasNext", true, csvImport.hasNext());
		assertEquals("Wrong result for hasNext", true, csvImport.hasNext());
		assertEquals("Wrong result for hasNext", true, csvImport.hasNext());
		assertEquals("Wrong result for hasNext", false, csvImport.hasNext());
	}

	@Test
	public void testInitializeReader() throws ClassNotFoundException {
		csvImport = instanceStrategy();

		csvImport.parse();

		verify(csvFileReader, times(1)).setConversionService(null);
		verify(csvFileReader, times(1)).setLineMessageFunction(any(Function.class));
		verify(csvFileReader, times(1)).setFileMetadataValidator(any(Optional.class));
	}

	@Test
	public void testParseLine() throws ClassNotFoundException {
		csvImport = instanceStrategy();
		csvImport.parse();

		verify(csvFileReader, times(1)).read(any(ReadFilter.class));
		verify(importer, times(1)).onParseLine(null);
	}

	private CsvImportStrategyBuilder.CsvImportStrategy instanceStrategy() throws ClassNotFoundException {
		CsvImportStrategyBuilder.CsvImportStrategy strategy = new CsvImportStrategyBuilder<GenericTestInstance, GenericTestCsvLine>() {
		}.new CsvImportStrategy(GenericTestInstance.class, GenericTestCsvLine.class, version -> (row -> new GenericTestInstance()),
				conversionService, (Consumer<MultipartFile>) multipartFile -> {
				}, ImportStrategy.PostProcessCondition.ON_EACH_LINE, null) {
			@Override
			public CsvFileReader initializeReader(MultipartFile file) {
				return TestCsvImportStrategy.this.csvFileReader;
			}
		};
		strategy.reinitialize(file);

		return strategy;
	}

}
