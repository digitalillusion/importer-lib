package xyz.deverse.importer.importer.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;
import org.springframework.web.multipart.MultipartFile;

import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.deverse.importer.ActionType;
import xyz.deverse.importer.ImportLine;
import xyz.deverse.importer.ReadFilter;
import xyz.deverse.importer.csv.CsvColumn;
import xyz.deverse.importer.csv.CsvFileReader;

public class TestCsvFileReader  {

	public static final String SHEET_NAME = "Sheer sheet";

	@Data
	@EqualsAndHashCode(callSuper=false)
	public static class TestCsvLine extends CsvFileReader.CsvLine<TestNode> {

		@CsvColumn(0)
		String cell0;

		@CsvColumn(1)
		String cell1;

		@CsvColumn(2)
		Integer cell2;

		@CsvColumn(value = 3, actionType = ActionType.DELETE)
		String cell3;

		@CsvColumn(value = 4, actionType = ActionType.IGNORE)
		String cell4;
	}

	public static class TestCsvRowMapper implements CsvFileReader.CsvImportMapper<TestNode, TestCsvLine> {

		@Override
		public int skipLines() {
			return skipLines;
		}

		@Override
		public TestNode toNode(TestCsvLine row) {
			TestNode node = new TestNode();
			node.field0 = row.cell0;
			node.field1 = row.cell1;
			node.field2 = row.cell2;
			return node;
		}
	}

	@Data
	public static class TestNode  {

		String field0;

		String field1;

		int field2;
	}

	SXSSFCell cell1 = mock(SXSSFCell.class);
	SXSSFCell cell2 = mock(SXSSFCell.class);
	SXSSFCell cell3 = mock(SXSSFCell.class);
	SXSSFCell cell4 = mock(SXSSFCell.class);
	SXSSFCell cell5 = mock(SXSSFCell.class);

	SXSSFCell cellEOF1 = mock(SXSSFCell.class);
	SXSSFCell cellEOF2 = mock(SXSSFCell.class);
	SXSSFCell cellEOF3 = mock(SXSSFCell.class);
	SXSSFCell cellEOF4 = mock(SXSSFCell.class);
	SXSSFCell cellEOF5 = mock(SXSSFCell.class);

	SXSSFCell cellH1 = mock(SXSSFCell.class);
	SXSSFCell cellH2 = mock(SXSSFCell.class);
	SXSSFCell cellH3 = mock(SXSSFCell.class);
	SXSSFCell cellH4 = mock(SXSSFCell.class);
	SXSSFCell cellH5 = mock(SXSSFCell.class);

	CsvFileReader<TestNode, TestCsvLine> csvFileReader;
	TestCsvRowMapper mapper;
	SXSSFRow row1 = mock(SXSSFRow.class);

	SXSSFRow row2 = mock(SXSSFRow.class);
	SXSSFRow row3 = mock(SXSSFRow.class);
	SXSSFWorkbook workbook;

	MultipartFile mockFile;

	ReadFilter readFilter;

	Consumer<ImportLine> parsedLineConsumer;
	
	static int skipLines = 1;

	@Before
	public void setup() {
		mockFile = mock(MultipartFile.class);
		mapper = new TestCsvRowMapper();

		readFilter = new ReadFilter();
		readFilter.setGroups(Collections.singletonList(SHEET_NAME));
		readFilter.setRawData(new HashMap<>());
		readFilter.getRawData().put(SHEET_NAME, new HashMap<>());

		SXSSFSheet sheet = mock(SXSSFSheet.class);

		ArrayList<Cell> cellsH = new ArrayList<Cell>();
		cellsH.add(makeCell(sheet, cellH1, 0, "1"));
		cellsH.add(makeCell(sheet, cellH2, 1, "head1"));
		cellsH.add(makeCell(sheet, cellH3, 2, "2"));
		cellsH.add(makeCell(sheet, cellH4, 3, "delete"));
		cellsH.add(makeCell(sheet, cellH5, 4, "ignore"));
		readFilter.getRawData().get(SHEET_NAME).put(0, () -> cellsH.stream().map(Cell::getStringCellValue).iterator());

		ArrayList<Cell> cells = new ArrayList<Cell>();
		cells.add(makeCell(sheet, cell1, 0, "cell1"));
		cells.add(makeCell(sheet, cell2, 1, "cell2"));
		cells.add(makeCell(sheet, cell3, 2, "3"));
		cells.add(makeCell(sheet, cell4, 3, "N"));
		cells.add(makeCell(sheet, cell5, 4, "N"));
		readFilter.getRawData().get(SHEET_NAME).put(1, () -> cells.stream().map(Cell::getStringCellValue).iterator());

		ArrayList<Cell> cellsEOF = new ArrayList<Cell>();
		cellsEOF.add(makeCell(sheet, cellEOF1, 0, ""));
		cellsEOF.add(makeCell(sheet, cellEOF2, 1, ""));
		cellsEOF.add(makeCell(sheet, cellEOF3, 2, ""));
		cellsEOF.add(makeCell(sheet, cellEOF4, 3, ""));
		cellsEOF.add(makeCell(sheet, cellEOF5, 4, ""));
		readFilter.getRawData().get(SHEET_NAME).put(5, () -> cellsEOF.stream().map(Cell::getStringCellValue).iterator());

		ArrayList<Row> rows = new ArrayList<Row>();
		rows.add(makeRow(sheet, row1, 0, cellsH));
		rows.add(makeRow(sheet, row2, 1, cells));
		rows.add(makeRow(sheet, row3, 5, cellsEOF));

		csvFileReader = new CsvFileReader<TestNode, TestCsvLine>(mockFile, TestCsvLine.class, new LinkedList<>(), new LinkedList<>(), version -> mapper) {
			@Override
			public void onParseLine(ImportLine parsedLine) {
				if (parsedLineConsumer != null) {
					parsedLineConsumer.accept(parsedLine);
				}
			}

			@Override
			public ReadFilter createFilter() {
				return null;
			}

			@SuppressWarnings("unchecked")
			@Override
			public Workbook initializeWorkbook() throws IOException, InvalidFormatException {

				workbook = mock(SXSSFWorkbook.class);

				when(sheet.getSheetName()).thenReturn(SHEET_NAME);
				when(sheet.getPhysicalNumberOfRows()).thenReturn(5);
				when(sheet.rowIterator()).thenReturn(rows.iterator());
				when(sheet.getWorkbook()).thenReturn(workbook);


				when(workbook.sheetIterator()).thenReturn((Iterator) Collections.singleton(sheet).iterator());
				when(workbook.getSheetAt(0)).thenReturn(sheet);
				when(workbook.getSheetIndex(SHEET_NAME)).thenReturn(0);
				when(workbook.getNumberOfSheets()).thenReturn(1);

				return workbook;
			}
		};
	}

	@Test
	public void testFatalException() {
		final String exceptionMessage = "Cannot open file";
		csvFileReader = new CsvFileReader<TestNode, TestCsvLine>(mockFile, TestCsvLine.class, new LinkedList<>(), new LinkedList<>(), version -> mapper) {
			@Override
			public void onParseLine(ImportLine parsedLine) {
			}

			@Override
			public ReadFilter createFilter() {
				return null;
			}

			public Workbook initializeWorkbook() throws IOException, InvalidFormatException {
				throw new IOException(exceptionMessage);
			}
		};

		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 1, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index", 0, testRecord.getIndex());
		assertTrue("Node should be absent", testRecord.getNodes().isEmpty());
		assertEquals("Wrong severity", Level.ERROR, testRecord.getSeverity());
		assertTrue("Exception class missing", testRecord.getMessage().contains(IOException.class.getCanonicalName()));
		assertTrue("Exception message missing", testRecord.getMessage().contains(exceptionMessage));
	}

	@Test
	public void testParseLineException() {
		final String exceptionMessage = "Panic!!!";
		parsedLineConsumer = parsedLine -> {
			if (parsedLine.getSeverity() != Level.ERROR) {
				throw new RuntimeException(exceptionMessage);
			}
		};

		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 3, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index", 0, testRecord.getIndex());
		assertTrue("Node should be absent", testRecord.getNodes().isEmpty());
		assertEquals("Wrong severity", Level.ERROR, testRecord.getSeverity());
		assertTrue("Exception class missing", testRecord.getMessage().contains(RuntimeException.class.getCanonicalName()));
		assertTrue("Exception message missing", testRecord.getMessage().contains(exceptionMessage));
	}

	@Test
	public void testHeadersOnFirstLine() {
		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 3, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index for record 0", 0, testRecord.getIndex());
		assertTrue("Node should be absent for record 0", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 0", "The table has headers", testRecord.getMessage());
		assertEquals("Wrong severity for record 0", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(1);
		assertEquals("Wrong line index for record 1", 1, testRecord.getIndex());
		assertFalse("Node should be present for record 1", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 1", "Imported instance", testRecord.getMessage());
		assertEquals("Wrong severity for record 1", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(2);
		assertEquals("Wrong line index for record 2", 5, testRecord.getIndex());
		assertTrue("Node should be absent for record 2", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 2", "Row at index 6 is empty (EOF)", testRecord.getMessage());
		assertEquals("Wrong severity for record 2", Level.INFO, testRecord.getSeverity());
	}

	@Test
	public void testNoHeadersOnFirstLine() {
		skipLines = -1;
		
		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 3, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index for record 0", 0, testRecord.getIndex());
		assertFalse("Node should be present for record 0", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 0", "Imported instance", testRecord.getMessage());
		assertEquals("Wrong severity for record 0", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(1);
		assertEquals("Wrong line index for record 1", 1, testRecord.getIndex());
		assertFalse("Node should be present for record 1", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 1", "Imported instance", testRecord.getMessage());
		assertEquals("Wrong severity for record 1", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(2);
		assertEquals("Wrong line index for record 2", 5, testRecord.getIndex());
		assertTrue("Node should be absent for record 2", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 2", "Row at index 6 is empty (EOF)", testRecord.getMessage());
		assertEquals("Wrong severity for record 2", Level.INFO, testRecord.getSeverity());
	}

	@Test
	public void testParseErrorOnSecondLine() {
		when(cell3.getRichStringCellValue().getString()).thenReturn("537,29");
		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 3, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index for record 0", 0, testRecord.getIndex());
		assertTrue("Node should be absent for record 0", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 0", "The table has headers", testRecord.getMessage());
		assertEquals("Wrong severity for record 0", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(1);
		assertTrue("Node should be absent", testRecord.getNodes().isEmpty());
		assertEquals("Wrong severity", Level.ERROR, testRecord.getSeverity());
		assertTrue("Exception message missing", testRecord.getMessage().contains(NumberFormatException.class.getCanonicalName()));
		testRecord = records.get(2);
		assertEquals("Wrong line index for record 2", 5, testRecord.getIndex());
		assertTrue("Node should be absent for record 2", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 2", "Row at index 6 is empty (EOF)", testRecord.getMessage());
		assertEquals("Wrong severity for record 2", Level.INFO, testRecord.getSeverity());
	}

	@Test
	public void testParseIgnoreLine() {
		when(cell5.getRichStringCellValue().getString()).thenReturn("Y");
		List<ImportLine> records = csvFileReader.read(readFilter).collect(Collectors.toList());
		assertEquals("Wrong line count", 3, records.size());
		ImportLine testRecord = records.get(0);
		assertEquals("Wrong line index for record 0", 0, testRecord.getIndex());
		assertTrue("Node should be absent for record 0", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 0", "The table has headers", testRecord.getMessage());
		assertEquals("Wrong severity for record 0", Level.INFO, testRecord.getSeverity());
		testRecord = records.get(1);
		assertEquals("Wrong line index for record 1", 1, testRecord.getIndex());
		assertTrue("Node should be empty for record 1", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 1", "Row at index 2 is ignored", testRecord.getMessage());
		assertEquals("Wrong severity for record 1", Level.WARN, testRecord.getSeverity());
		testRecord = records.get(2);
		assertEquals("Wrong line index for record 2", 5, testRecord.getIndex());
		assertTrue("Node should be absent for record 2", testRecord.getNodes().isEmpty());
		assertEquals("Wrong line message for record 2", "Row at index 6 is empty (EOF)", testRecord.getMessage());
		assertEquals("Wrong severity for record 2", Level.INFO, testRecord.getSeverity());
	}

	private Cell makeCell(SXSSFSheet sheet, Cell cell, int columnIndex, String text) {
		when(cell.getCellType()).thenReturn(CellType.STRING);
		RichTextString richText = mock(RichTextString.class);
		when(richText.getString()).thenReturn(text);
		when(cell.getRichStringCellValue()).thenReturn(richText);
		when(cell.getStringCellValue()).thenReturn(text);
		when(cell.getColumnIndex()).thenReturn(columnIndex);
		when(cell.getSheet()).thenReturn(sheet);
		return cell;
	}

	private Row makeRow(SXSSFSheet sheet, Row row, int rowNum, Collection<Cell> cells) {
		when(row.getRowNum()).thenReturn(rowNum);
		when(row.cellIterator()).thenReturn(cells.iterator());
		when(row.getSheet()).thenReturn(sheet);
		return row;
	}

}
