/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */


package io.strimzi.kafka.bridge.contentRouting;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.kafka.bridge.Application;

public class DependencyLoader {

    private static final String TEMP_ZIP_FILE = "/tmp/downloadingZip.zip";
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static Map<String, CustomClassLoader> map = new HashMap<String, CustomClassLoader>();
    private static final String JAR_EXTENSION = ".jar";

    /**
     * 
     * @param sourceUrl the the URL at which the ZIP file can be downloaded
     * @param customClass name of the custom class
     * @param destDirString is the name of the folder to which the ZIP file is going to be extracted
     * @param isCondition is set to true if the class only checks for a condition. If set to false, the class is meant to return the name of a topic
     * @return instance of the custom class `customClass`
     * @throws Exception
     */
    public static Object createObject(String sourceUrl, String customClass, String destDirString, boolean isCondition) 
            throws Exception {

        
        // if a folder with the same name exists already, delete it (it might happen only if some error occurred when downloading other zip files)
        File destDir = new File(destDirString);
        deleteFolder(destDir);

        try {
            URL[] classUrls;
            CustomClassLoader cll;

            if (map.containsKey(sourceUrl)) {
                
                // the ZIP file has already been downloaded
                cll = map.get(sourceUrl);
                log.info(sourceUrl + " was already downlaoded");
                
            } else {
            
                // create the directory, in which the content of the ZIP file is going to be extracted
                destDir.mkdir();
                
                //download zip archive from provided url
                downloadFromUrl(new URL(sourceUrl));
                
                // zip archive is downloaded to TEMP_ZIP_FILE, we can unzip it
                classUrls = unzipArchive(destDir);
                cll = new CustomClassLoader(classUrls, null);
                map.put(sourceUrl, cll);
                
                // delete the temp zip archive
                new File(TEMP_ZIP_FILE).delete();
                
            }
            
            Class<?> clazz = cll.loadClass(customClass);
            
            if (isCondition) {
                Object condition = clazz.newInstance();
                
                clazz.getMethod("isSatisfied", Map.class, String.class);
                log.info("Class " + clazz.getCanonicalName() + " successfully instantiated");
                
                return condition;
            } else {
                Object router = clazz.newInstance();
                clazz.getMethod("getTopic", Map.class, String.class);
                log.info("Class " + clazz.getCanonicalName() + " successfully instantiated");
                
                return router;
            }

        } catch (Exception e) {
            deleteFolder(destDir);
            Files.deleteIfExists(new File(TEMP_ZIP_FILE).toPath());
            throw e;
        }
    }

    private static void downloadFromUrl(URL downloadUrl) throws Exception {

        ReadableByteChannel readableByteChannel = Channels.newChannel(downloadUrl.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(TEMP_ZIP_FILE);
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);

        fileOutputStream.close();
        fileChannel.close();

    }

    private static URL[] unzipArchive(File destDir) throws Exception {

        ArrayList<URL> classUrls = new ArrayList<URL>();

        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(TEMP_ZIP_FILE));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(destDir, zipEntry);
            
            if (getFileExtension(newFile.getName()).equals(JAR_EXTENSION)) {
                classUrls.add(new URL("file:" + destDir + "/" + newFile.getName()));
            }
            FileOutputStream fos = new FileOutputStream(newFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
        URL[] urlsArray = new URL[classUrls.size()];
        urlsArray = classUrls.toArray(urlsArray);
        return urlsArray;

    }

    private static File newFile(File destinationDir, ZipEntry zipEntry) throws Exception {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new Exception("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }
    
    public static void deleteFolder(File destDir) {
        if (destDir.exists()) {
            String[] files = destDir.list();
            for (String f : files) {
                File currentFile = new File(destDir.getPath(), f);
                currentFile.delete();
            }
            destDir.delete();                
        }
    }
    
    private static String getFileExtension(String name) {
        return name.substring(name.length() - 4);
    }
}
