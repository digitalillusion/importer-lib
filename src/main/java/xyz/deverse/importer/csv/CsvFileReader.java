package xyz.deverse.importer.csv;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

import com.sun.org.apache.xpath.internal.compiler.FunctionTable;
import xyz.deverse.importer.*;
import xyz.deverse.importer.misc.DefaultValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.multipart.MultipartFile;

import lombok.Getter;
import lombok.Setter;
import xyz.deverse.importer.csv.CsvFileReader.CsvLine;

public abstract class CsvFileReader<T, S extends CsvLine<T>> extends FileReader<Row, S> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CsvFileReader.class);
	private Integer version;

	public static interface CsvImportMapper<T, S extends CsvLine<T>> extends ImportMapper<T, S> { }

	@Getter
	@Setter
	public static class CsvLine<T> implements ImportLine {
		int count;
		ActionType actionType = ActionType.PERSIST;
		String group;
		List<String> groups;
		int index;
		int indexInGroup;
		List<T> nodes = new ArrayList<>();
		Set<Long> excludedIds = new HashSet<>();
		String message;
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
			this.setNodes(new ArrayList<>());
			return this;
		}
	}

	ConversionService conversionService;

	DataFormatter dataFormatter;

	@Setter
	Function<String, Boolean> emptyCellValueEvaluatorFunction;

	@Setter
	Function<Integer, String> emptyLineMessageFunction;

	@Setter
	Function<Integer, String> ignoredLineMessageFunction;


	@Setter
	Optional<Consumer<MultipartFile>> fileMetadataValidator;

	@Setter
	Supplier<String> headerMessageSupplier;

	@Setter
	Function<T, String> lineMessageFunction;

	Function<Integer,CsvFileReader.CsvImportMapper<T, S>> mapper;

	@SuppressWarnings("unchecked")
	public CsvFileReader(MultipartFile file, Class<S> lineType, LinkedList<Consumer<ImportLine>> lineProcessors, Collection<ImportLine> importedLines, Function<Integer,CsvFileReader.CsvImportMapper<T, S>> mapper) {
		super(file, lineType, lineProcessors, importedLines);
		initialize(mapper);
	}

	@Override
	public ReadFilter createFilter() {
		try(Workbook workbook = initializeWorkbook()) {
			ReadFilter filter = new ReadFilter();
			filter.setGroups(new ArrayList<>());
			filter.setRawData(new HashMap<>());
			getSheetNames(workbook).forEach(name -> {
				filter.setFilename(file.getOriginalFilename());
				filter.getRawData().put(name, new HashMap<>());
				workbook.getSheet(name).rowIterator().forEachRemaining(row -> {
					Stream<String> stream = StreamSupport.stream(
					Spliterators.spliteratorUnknownSize(
							row.cellIterator(),
							Spliterator.ORDERED)
					, false).map(cell -> extractCellValue(cell).toString());
					filter.getRawData().get(name).put(row.getRowNum(), () -> stream.iterator());
				});
				filter.getGroups().add(name);
			});
			String firstCellOnFirstSheet = filter.getRawData().get(filter.getGroups().get(0)).get(0).iterator().next();
			filter.setVersion(Integer.parseInt(firstCellOnFirstSheet));
			return filter;
		} catch (Exception e) {
			throw new UnsupportedOperationException("Cannot create filter ", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<ImportLine> read(ReadFilter filter) {
		List<String> sheetNames = new ArrayList<>();
		try(Workbook workbook = initializeWorkbook()) {
			sheetNames.addAll(getSheetNames(workbook));
			Iterator<Row> iterator = StreamSupport //
					.stream(Spliterators.spliteratorUnknownSize(workbook.sheetIterator(), Spliterator.ORDERED), false)//
					.flatMap(s -> StreamSupport //
							.stream(Spliterators.spliteratorUnknownSize(s.rowIterator(), Spliterator.ORDERED), false))
					.filter(row -> filter.getGroups().contains(row.getSheet().getSheetName()))
					.filter(row -> filter.getRawData().get(row.getSheet().getSheetName()).keySet().contains(row.getRowNum()))
					.iterator();
			setIterator(iterator);

			Map<Integer, Integer> processedRowsCountPerSheet = new HashMap<>();
			for (int i = 0; i <= workbook.getNumberOfSheets(); i++) {
				int rowCount = i > 0 ? processedRowsCountPerSheet.get(i - 1) : 0;
				if (i > 0 && i <= workbook.getNumberOfSheets()) {
					String sheetName = workbook.getSheetAt(i - 1).getSheetName();
					if (filter.getGroups().contains(sheetName)) {
						rowCount = rowCount + filter.getRawData().get(sheetName).keySet().size();
					}
				}
				processedRowsCountPerSheet.put(i, rowCount);
			}

			return StreamSupport //
					.stream(Spliterators.spliteratorUnknownSize(getIterator(), Spliterator.ORDERED), false)
					.map(row -> parseRow(row, processedRowsCountPerSheet)) //
					.map(line -> {
						try {
							this.onParseLine(line);
							if (!line.getNodes().isEmpty()) {
								return ImportMapper.MappedLine.<T> builder()//
										.group(line.getGroup())//
										.index(line.getIndex())//
										.indexInGroup(line.getIndexInGroup())//
										.count(line.getCount())//
										// Call line message function after parse line so that node is rehydrated
										.message(line.getNodes().stream().map(it -> lineMessageFunction.apply((T) it)).collect(Collectors.joining("\n")))//
										.excludedIds(Collections.emptySet())
										.nodes((List<T>) line.getNodes())//
										.severity(line.getSeverity())//
										.saveDepth(line.getSaveDepth())//
										.build();
							}
							return line;
						} catch (RuntimeException e) {
							LOGGER.error(e.getMessage(), e);
							ImportLine errorLine = makeErrorLine(Level.ERROR, e, line.getGroup(), line.getIndex(), line.getIndexInGroup(), line.getCount(), sheetNames);
							this.onParseLine(errorLine);
							return errorLine;
						}
					});

		} catch (Exception e) {
			ImportLine[] errorLines = new ImportLine[] { makeErrorLine(Level.ERROR, e, sheetNames) };
			return StreamSupport //
					.<ImportLine> stream(Spliterators.spliterator(errorLines, Spliterator.ORDERED), false) //
					.peek(this::onParseLine);
		}
	}

	private List<String>  getSheetNames(Workbook workbook) {
		List<String> sheetNames = new ArrayList<>();
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			sheetNames.add(workbook.getSheetName(i));
		}
		return sheetNames;
	}

	public void setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	public void skip(String currentGroupName) {
		while (getIterator().hasNext()) {
			Row row = getIterator().next();
			if (!currentGroupName.equals(row.getSheet().getSheetName())) {
				return;
			}
		}
	}

	private Object extractCellValue(Cell cell) {
		Object cellValue;
		try {
			// do not evaluate formulas
			cellValue = CellType.FORMULA == cell.getCellType() ? cell.getStringCellValue() : dataFormatter.formatCellValue(cell);
		} catch (NumberFormatException | IllegalStateException ex) {
			cellValue = cell.getNumericCellValue();
		}
		return cellValue;
	}

	private boolean fillHeadersOnFirstRow(Row row) {
		if (row.getRowNum() > 0 || row.getRowNum() > mapper.apply(version).skipLines()) {
			return false;
		} else if (row.getRowNum() == 0) {
			Iterator<Cell> cellIterator = row.cellIterator();
			List<String> sheetHeaders = new ArrayList<>();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				sheetHeaders.add(cell.getStringCellValue());
			}
			Sheet sheet = row.getSheet();
			version = Integer.parseInt(sheetHeaders.get(0));
			headers.add(sheet.getWorkbook().getSheetIndex(sheet), sheetHeaders);
		}
		return true;
	}

	private void initialize(Function<Integer, CsvImportMapper<T, S>> mapper) {
		Function<Integer, CsvImportMapper<T, S>> defaultMapper = version -> new CsvImportMapper<T, S>() {
			@Override
			public boolean isNeeded() {
				return false;
			}

			@Override
			public T toNode(S row) {
				return null;
			}

			;
		};
		this.mapper = Optional.ofNullable(mapper).orElse(defaultMapper);
		this.lineMessageFunction = node -> "Imported instance";
		this.headerMessageSupplier = () -> "The table has headers";
		this.emptyLineMessageFunction = index -> "Row at index " + (index + 1) + " is empty (EOF)";
		this.ignoredLineMessageFunction = index -> "Row at index " + (index + 1) + " is ignored";
		this.emptyCellValueEvaluatorFunction = cellValue -> cellValue.isEmpty();
		this.conversionService = new DefaultConversionService();
		dataFormatter = new DataFormatter();
	}

	private CsvLine<T> makeErrorLine(Level logLevel, Exception e, List<String> sheetNames) {
		return makeErrorLine(logLevel, e, "", 0, 0, 1, sheetNames);
	}

	private CsvLine<T> makeErrorLine(Level logLevel, Exception e, String group, int index, int indexInGroup, int count, List<String> sheetNames) {
		CsvLine<T> errorLine = new CsvLine<>();
		errorLine.group = group;
		errorLine.groups = sheetNames;
		errorLine.count = count;
		errorLine.index = index;
		errorLine.indexInGroup = indexInGroup;
		errorLine.message = e.getClass().getCanonicalName() + " " + e.getMessage();
		errorLine.excludedIds = Collections.emptySet();
		errorLine.severity = logLevel;
		return errorLine;
	}

	private boolean mapRowToCsvLine(List<String> headers, Row row, S csvLine) throws FileReaderException {

		Iterator<Cell> cellIterator = row.cellIterator();
		List<Field> csvLineFields = FieldUtils.getFieldsListWithAnnotation(csvLine.getClass(), CsvColumn.class);

		boolean isEmptyRow = true;
		while (cellIterator.hasNext()) {
			Cell cell = cellIterator.next();
			Object cellValue = extractCellValue(cell);

			boolean isEmptyCell = emptyCellValueEvaluatorFunction.apply(cellValue.toString());
			isEmptyRow &= isEmptyCell;

			if (isEmptyCell) {
				continue;
			}

			for (Field field : csvLineFields) {
				if (field.getAnnotation(CsvColumn.class).value() == cell.getColumnIndex()) {
					try {
						Object value = conversionService.convert(cellValue, field.getType());
						boolean isAccessible = field.isAccessible();
						field.setAccessible(true);
						ReflectionUtils.setField(field, csvLine, value);
						field.setAccessible(isAccessible);
						break;
					} catch (Exception e) {
						LOGGER.error("Error setting field " + getLineType().getCanonicalName() + "." + field.getName() + " (" + field.getType() + ") with value " + cellValue);
						String header = headers.size() > cell.getColumnIndex() ? headers.get(cell.getColumnIndex()) : String.valueOf(cell.getColumnIndex());
						throw new FileReaderException(header, cell.getRowIndex(), e);
					}
				}
			}
		}
		return isEmptyRow;
	}

	private ImportLine parseRow(Row row, Map<Integer, Integer> processedRowsCountPerSheet) {
		Sheet sheet = row.getSheet();
		Workbook workbook = sheet.getWorkbook();
		List<String> sheetNames = getSheetNames(workbook);
		Integer totalRowCount = processedRowsCountPerSheet.get(workbook.getNumberOfSheets());
		Integer rowNum = processedRowsCountPerSheet.get(workbook.getSheetIndex(sheet)) + row.getRowNum();
		try {
			ImportLine importerLine;
			if (fillHeadersOnFirstRow(row)) {
				importerLine = parseRowForHeadersLine(row, sheet, totalRowCount, rowNum, sheetNames);
			} else {
				S csvLine = getLineType().newInstance();
				List<String> sheetHeaders = new ArrayList<>();
				int sheetIndex = workbook.getSheetIndex(sheet);
				if (headers.size() > sheetIndex) {
					sheetHeaders = headers.get(sheetIndex);
				}

				if (!mapRowToCsvLine(sheetHeaders, row, csvLine)) {
					importerLine = parseRowForCsvLine(row, sheet, totalRowCount, rowNum, csvLine, sheetNames);
					if (ActionType.IGNORE.equals(importerLine.getActionType())) {
						importerLine = parseRowForIgnoredLine(row, sheet, rowNum, sheetNames);
					}
				} else {
					importerLine = parseRowForEmptyLine(row, sheet, rowNum, sheetNames);
				}
			}
			return importerLine;
		} catch (Exception e) {
			return makeErrorLine(Level.ERROR, e, sheet.getSheetName(), rowNum, row.getRowNum(), totalRowCount, sheetNames);
		}
	}

	public ActionType getActionType(S csvLine){
		BiFunction<Field, S, Object> wrap = (f, line) -> {
			try {
				return f.get(line);
			} catch (IllegalAccessException iae) {
				LOGGER.error(iae.getMessage(), iae);
			}
			return null;
		};

		return Stream.of(csvLine.getClass().getDeclaredFields())
				.map(field -> {
					field.setAccessible(true);
					return field;
				})
				.filter(f -> Optional.ofNullable(wrap.apply(f, csvLine))
						.filter(value -> CharSequence.class.isAssignableFrom(value.getClass()))
						.map(CharSequence.class::cast)
						.map(value -> StringUtils.equalsAnyIgnoreCase(value, DefaultValue.BOOLEAN_FLAGS))
						.orElse(false) && f.getAnnotation(CsvColumn.class).actionType() != ActionType.PERSIST)
				.map(f -> f.getAnnotation(CsvColumn.class))
				.sorted(Comparator.comparingInt(column -> column.actionType().getOrder()))
				.map(column -> column.actionType())
				.findFirst()
				.orElse(ActionType.PERSIST);
	}

	private ImportLine parseRowForCsvLine(Row row, Sheet sheet, Integer totalRowCount, Integer rowNum, S csvLine, List<String> sheetNames) {
		ImportLine importerLine;
		if (mapper.apply(version).isNeeded()) {
			try {
				T node = mapper.apply(version).toNode(csvLine);
				importerLine = ImportMapper.MappedLine.<T> builder()//
						.actionType(getActionType(csvLine))
						.count(totalRowCount)//
						.group(sheet.getSheetName())//
						.groups(sheetNames)
						.index(rowNum) //
						.indexInGroup(row.getRowNum() + 1) //
						.excludedIds(Collections.emptySet())
						.nodes(new ArrayList<>(Arrays.asList(node)))//
						.severity(Level.INFO)//
						.saveDepth(new AtomicInteger(0))//
						.build();
			} catch (Exception e) {
				// Dont forget whe are already on the next line
				row.setRowNum(row.getRowNum() + 1);
				LOGGER.error("Mapper " + mapper.getClass().getSimpleName() + " invocation error: ", e);
				throw new RuntimeException("Mapper " + mapper.getClass().getSimpleName() + " invocation error: " + e.getMessage());
			}
		} else {
			csvLine.actionType = getActionType(csvLine);
			csvLine.count = totalRowCount;
			csvLine.group = sheet.getSheetName();
			csvLine.groups = sheetNames;
			csvLine.index = rowNum;
			csvLine.indexInGroup = row.getRowNum() + 1;
			csvLine.severity = Level.INFO;
			csvLine.saveDepth = new AtomicInteger(0);
			csvLine.excludedIds = Collections.emptySet();
			importerLine = csvLine;
		}
		return importerLine;
	}

	private ImportLine parseRowForEmptyLine(Row row, Sheet sheet, Integer rowNum, List<String> sheetNames) {
		CsvLine<T> importerLine = new CsvLine<>();
		importerLine.count = rowNum;
		importerLine.group = sheet.getSheetName();
		importerLine.groups = sheetNames;
		importerLine.index = rowNum;
		importerLine.indexInGroup = row.getRowNum() + 1;
		importerLine.message = emptyLineMessageFunction.apply(rowNum);
		importerLine.severity = Level.INFO;
		importerLine.saveDepth = new AtomicInteger(0);
		importerLine.excludedIds = Collections.emptySet();
		Workbook workbook = sheet.getWorkbook();
		if (workbook.getSheetIndex(sheet.getSheetName()) < workbook.getNumberOfSheets()) {
			skip(sheet.getSheetName());
		}
		return importerLine;
	}


	private ImportLine parseRowForIgnoredLine(Row row, Sheet sheet, Integer rowNum, List<String> sheetNames) {
		CsvLine<T> importerLine = new CsvLine<>();
		importerLine.count = rowNum;
		importerLine.group = sheet.getSheetName();
		importerLine.groups = sheetNames;
		importerLine.index = rowNum;
		importerLine.indexInGroup = row.getRowNum() + 1;
		importerLine.message = ignoredLineMessageFunction.apply(rowNum);
		importerLine.severity = Level.WARN;
		importerLine.saveDepth = new AtomicInteger(0);
		importerLine.excludedIds = Collections.emptySet();
		return importerLine;
	}

	private ImportLine parseRowForHeadersLine(Row row, Sheet sheet, Integer totalRowCount, Integer rowNum, List<String> sheetNames) {
		CsvLine<T> importerLine = new CsvLine<>();
		importerLine.count = totalRowCount;
		importerLine.group = sheet.getSheetName();
		importerLine.groups = sheetNames;
		importerLine.index = rowNum;
		importerLine.indexInGroup = row.getRowNum() + 1;
		importerLine.message = headerMessageSupplier.get();
		importerLine.severity = Level.INFO;
		importerLine.saveDepth = new AtomicInteger(0);
		importerLine.excludedIds = Collections.emptySet();
		return importerLine;
	}

	public Workbook initializeWorkbook() throws IOException, InvalidFormatException {
		fileMetadataValidator.ifPresent(validator -> validator.accept(getFile()));
		Workbook workbook = WorkbookFactory.create(getFile().getInputStream());
		return workbook;
	}
}

