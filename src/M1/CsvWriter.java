package M1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.io.PrintWriter;

public class CsvWriter {
    private BufferedWriter writer;
    public BufferedWriter getWriter() {
		return writer;
	}

	public void setWriter(BufferedWriter writer) {
		this.writer = writer;
	}

	private static boolean headersWritten = false;

    public CsvWriter(File file) throws DBAppException {
		if(!headersWritten) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader("MetaData.csv"));
				String line = br.readLine();
				headersWritten = (line != null);
				br.close();
			} catch ( IOException e) {
				throw new DBAppException("Error reading csv file");    
			}
		}
    }

    public void writeHeaders(List<String> headers) throws DBAppException {
        if (headersWritten) {
            return;
        }
		try {
	        File file = new File("MetaData.csv");
	    	FileWriter fr;
				fr = new FileWriter(file, false);
			
	    	PrintWriter printWriter = new PrintWriter(fr);
	    	StringBuilder stringBuilder = new StringBuilder();
	    	for( String token: headers){
	    	stringBuilder.append(token);
	    	stringBuilder.append(',');
	    	}
	    	stringBuilder.deleteCharAt(stringBuilder.length() - 1);
	    	stringBuilder.append('\n');
	    	printWriter.write(stringBuilder.toString());
	    	printWriter.flush();
	    	printWriter.close();
        headersWritten = true;
    	} catch (IOException e) {
			throw new DBAppException("Error modifying CSV file");
		}
    }
    
    
    public void writeRow(List<String> data) throws DBAppException {
        if (!headersWritten) 
            throw new DBAppException("Headers of CSV file have not been written yet.");
        try {
			File file = new File("MetaData.csv");
			FileWriter fr = new FileWriter(file, true);
			PrintWriter printWriter = new PrintWriter(fr);
			StringBuilder stringBuilder = new StringBuilder();
			for (String token : data) {
				stringBuilder.append(token);
				stringBuilder.append(',');
			}
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
			stringBuilder.append('\n');
			printWriter.write(stringBuilder.toString());
			printWriter.flush();
			printWriter.close();
        } catch (IOException e) {
			throw new DBAppException(e.getMessage());
		}
    }
    
    
    public void writeRow(List<String> data, BufferedReader br) throws IOException {
//        if (!headersWritten) {
//            throw new IOException("Headers have not been written yet.");
//        }
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();
    	while (line != null) {
    		sb.append(line);
    		writer.newLine();
    	}
        for (String datum : data) {
            sb.append(datum).append(",");
        }
        sb.deleteCharAt(sb.length() - 1); // Remove the last comma
        writer.write(sb.toString());
        writer.newLine();
    }
    
    

	public static boolean isHeadersWriten() {
		return headersWritten;
	}

    public void close() throws IOException {
        writer.close();
    }
}
