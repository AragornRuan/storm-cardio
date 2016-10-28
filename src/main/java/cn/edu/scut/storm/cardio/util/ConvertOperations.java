package cn.edu.scut.storm.cardio.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Scanner;

public class ConvertOperations {
	
	private static final char COMMA = ',';
	private static final char SPACE = ' '; 
	private static final String SEMICOLON = ";";
	private static final String LEFT_SQUARE = "[";
	private static final String RIGHT_SQUARE = "]";	
	/***
	 * 将ECG数据从Json的格式转化为CDG C++ 计算程序中fmat对象的构造函数参数格式
	 * @param ecgJson json格式 [\n[1,2,3],\n[4,5,6]\n]
	 * @return fmat对象的构造函数参数格式 1 2 3;4 5 6
	 * @throws IOException
	 */
	public static String EcgJsonToFmat(String ecgJson) throws IOException {
		BufferedReader reader = new BufferedReader(new StringReader(ecgJson));
		String line = null;
		StringBuilder builder = new StringBuilder();
		while((line = reader.readLine()) != null) {
			int start = line.indexOf(LEFT_SQUARE);
			int end = line.indexOf(RIGHT_SQUARE);
			if (start == -1 || end == -1) {
				continue;
			}
			
			line = line.substring(start + 1, end);
			line = line.replace(COMMA, SPACE);
			builder.append(line).append(SEMICOLON);
		}
		
		return builder.toString();
	}

	public static String CdgToJson(String cdg) throws IOException {
		BufferedReader reader = new BufferedReader(new StringReader(cdg));
		String line = null;
		StringBuilder builder = new StringBuilder();
		builder.append(LEFT_SQUARE);
		while ((line = reader.readLine()) != null) {
			builder.append(LEFT_SQUARE);
			Scanner scanner = new Scanner(new StringReader(line));
			while (scanner.hasNext()) {
				String word = scanner.next();
				builder.append(word).append(COMMA);
			}
			scanner.close();
			builder.append(RIGHT_SQUARE);
		}
		builder.append(RIGHT_SQUARE);
		return builder.toString();
	}

}
