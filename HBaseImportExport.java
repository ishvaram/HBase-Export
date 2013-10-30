package hbaseexport;

public class HBaseImportExport {
public String DEFAULT_COLUMN_FAMILY = "c1";
	
	public class HBaseCol {
		String family;
		String qualifier;
		HBaseCol(String family,String qualifier){
			this.family = family;
			this.qualifier = qualifier;
		}
	}
}
