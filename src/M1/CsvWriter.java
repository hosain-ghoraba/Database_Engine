package M1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

	private boolean headersWritten;

    public CsvWriter(File file) throws IOException {
        writer = new BufferedWriter(new FileWriter(file));
        headersWritten = false;
    }

    public void writeHeaders(List<String> headers) throws IOException {
        if (headersWritten) {
            throw new IOException("Headers have already been written.");
        }
        File file = new File("MetaData.csv");
    	FileWriter fr = new FileWriter(file, false);
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
    }
    
    
    public void writeRow(List<String> data) throws IOException {
        if (!headersWritten) 
            throw new IOException("Headers have not been written yet.");
       File file = new File("MetaData.csv");
    	FileWriter fr = new FileWriter(file, true);
    	PrintWriter printWriter = new PrintWriter(fr);
    	StringBuilder stringBuilder = new StringBuilder();
    	for( String token: data){
    	stringBuilder.append(token);
    	stringBuilder.append(',');
    	}
    	stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    	stringBuilder.append('\n');
    	printWriter.write(stringBuilder.toString());
    	printWriter.flush();
    	printWriter.close();
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
    
    
    

    public void close() throws IOException {
        writer.close();
    }
}
