package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

public class MyAppSource {
    
	private static String localPath = Paths.get("src/main/resources/").toAbsolutePath().toString()+"\\";
	
	public void Download(String fileURL, String fileName){
		
		File file = new File(localPath+fileName+".csv");
		
		if (!file.exists()) {
			try {
				System.out.println("Downloading "+fileName+" ... ");
				URL url = new URL(fileURL);
				FileUtils.copyURLToFile(url, file);
				System.out.println("FINISHED Downloading "+fileName+" ... ");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("File "+fileName+" already exist no need to download");
		}
	}
	
	public File getDownloadedFile(String fileName){
		
		File file = new File(localPath+fileName+".csv");
		
		if (file.exists()) {
			return file;
		}
		return null;
	}
	
	
	
}
