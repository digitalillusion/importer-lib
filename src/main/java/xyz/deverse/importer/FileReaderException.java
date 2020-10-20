package xyz.deverse.importer;

import lombok.Getter;

public class FileReaderException extends Exception {

	private static final long serialVersionUID = 1L;
	private Exception exception;
	private String header;
	@Getter
	private Integer rowIndex;

	public FileReaderException(String header, int rowIndex, Exception exception) {
		this.rowIndex = rowIndex;
		this.header = header;
		this.exception = exception;
	}

	@Override
	public String getMessage() {
		return "Parse failed on field '" + header + "' with the following cause: " + exception.getClass().getCanonicalName() + " " + exception.getMessage();
	}
}
