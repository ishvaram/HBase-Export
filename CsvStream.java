package hbaseexport;

public class CsvStream {
	char delimiter = ',';
	String encoding = "UTF-8";
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
}

