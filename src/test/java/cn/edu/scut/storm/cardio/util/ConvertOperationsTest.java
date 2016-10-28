package cn.edu.scut.storm.cardio.util;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

public class ConvertOperationsTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConvertOperationsTest.class);
	
	private static final String ECG_JSON = "[\n[1.1,2.2,3.3],\n[4.4,5.5,6.6]\n]";
	private static final String ECG_FMAT = "1.1 2.2 3.3;4.4 5.5 6.6";
	private static final String CDG = "  1.1  2.2  3.3\n  4.4  5.5  6.6\n  7.7 8.8  9.9";
	private static final String CDG_JSON = "[[1.1,2.2,3.3],[4.4,5.5,6.6],[7.7,8.8,9.9]]";
	
	@Test
	void EcgJsonToFmatTest() {
		try {
			String ecgFmat = ConvertOperations.EcgJsonToFmat(ECG_JSON);
			Assert.assertEquals(ECG_FMAT, ecgFmat);
		}
		catch (IOException exception) {
			LOGGER.info("Convert ECG from json to fmat failed.");
		}
	}
	
	@Test
	void CdgToJsonTest() {
		try {
			String cdgJson = ConvertOperations.CdgToJson(CDG);
			Assert.assertEquals(CDG_JSON, cdgJson);
		}
		catch (IOException exception) {
			LOGGER.info("Convert CDG to json failed.");
		}
	}
}
