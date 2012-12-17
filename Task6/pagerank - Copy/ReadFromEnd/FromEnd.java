import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FromEnd {
	public static void read(String filename) {
		read(filename, "GBK");
	}

	public static void read(String filename, String charset) {

		RandomAccessFile rf = null;
		try {
			rf = new RandomAccessFile(filename, "r");
		    long len = rf.length();
		    long start = rf.getFilePointer();
		    long nextend = start + len - 1;
		    String line;
		    rf.seek(nextend);
		    int c = -1;
		    int count = 100;
		    
		    while (nextend > start) {
		    	
		    	c = rf.read();
		    	if (c == '\n' || c == '\r') {
		    		
		    		count --;
		    		if(count == 0)
		    			break;
		    		
		    		line = rf.readLine();
		    		if (line != null) {
		    			System.out.println(new String(line.getBytes("ISO-8859-1"), charset));
		    		}else {
		    			System.out.println(line);// 输出为null，可以注释掉
		    		}
		    		nextend--;
		    	}
		    	nextend--;
		    	rf.seek(nextend);
		    	if (nextend == 0 ) {// 当文件指针退至文件开始处，输出第一行
		    		System.out.println(new String(rf.readLine().getBytes("ISO-8859-1"), charset));
		
		    	}
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rf != null)
					rf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
  }

	public static void main(String args[]) {
		//read("/usr/local/hadoop/ct6", "gbk");
		read("e://chromdown/iter1", "gbk");
	}
}
