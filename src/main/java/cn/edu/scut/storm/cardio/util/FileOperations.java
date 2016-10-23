package cn.edu.scut.storm.cardio.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileOperations {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FileOperations.class);
	
	public static String readFile(String filePath) {
		File file = new File(filePath);
		StringBuilder stringBuilder = new StringBuilder();
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
			String tempContent;
			while ((tempContent = bufferedReader.readLine()) != null) {
				stringBuilder.append(tempContent);
				stringBuilder.append("\n");
			}
		} catch (Exception exception) {
			LOGGER.error("Read file {} error.", filePath);
			exception.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException ioException) {
					LOGGER.error("Close bufferedReader error.");
					ioException.printStackTrace();
				}
			}
		}
		
		return stringBuilder.toString();
	}
	
	public static String loadECG(File file) {
		StringBuilder stringBuilder = new StringBuilder();
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
			String tempContent;
			while ((tempContent = bufferedReader.readLine()) != null) {
				stringBuilder.append(tempContent);
				stringBuilder.append(";");
			}
		} catch (Exception exception) {
			LOGGER.error("Read file {} error.", file.getName());
			exception.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException ioException) {
					LOGGER.error("Close bufferedReader error.");
					ioException.printStackTrace();
				}
			}
		}
		
		return stringBuilder.toString();
	}
	
	public static List<File> listFile(String dirPath) {
		File directory = new File(dirPath);
		List<File> files = new ArrayList<File>();
/*		if (directory.isFile()) {
			files.add(directory);
			return files;
		}
		else if (directory.isDirectory()) {
			File[] fileArr = directory.listFiles();
			for (int i = 0; i < fileArr.length; i++) {
				files.addAll(listFile(fileArr[i].getName()));
			}
		}*/
		if (directory.isDirectory()) {
			for (File file : directory.listFiles()) {
				files.add(file);
			}
			LOGGER.debug("files' size is {}.", files.size());
		}
		else {
			LOGGER.error("List files from {} error.", dirPath);
		}
		return files;
	}
	
	public static void saveFile(String filename, String content) {
		BufferedWriter bufferedWriter = null;
		try {
			File file = new File(filename);
			if (!file.exists()) {
				file.createNewFile();
			}
			bufferedWriter = new BufferedWriter(new FileWriter(file));
			bufferedWriter.write(content);
			LOGGER.info("Write file {} successfully", filename);
		} catch (IOException ioException) {
			LOGGER.error("Write file {} error.", filename);
			ioException.printStackTrace();
		} finally {
			if (bufferedWriter != null) {
				try {
					bufferedWriter.close();
				} catch (IOException ioException) {
					LOGGER.error("Close bufferedWriter error.");
					ioException.printStackTrace();
				}
			}
		}
	}

}
