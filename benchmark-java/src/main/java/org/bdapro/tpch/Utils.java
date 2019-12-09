package org.bdapro.tpch;

import java.io.File;

class Utils {
    static File validateFile(String fileName, boolean shouldExist) {
        if (fileName == null) {
            throw new IllegalArgumentException("Missing file parameter");
        }
        File file = new File(fileName);
        if (shouldExist && (!file.exists() || file.isDirectory())) {
            throw new IllegalArgumentException(fileName + " file not found: " + file.getAbsolutePath());
        }
        if (!shouldExist && file.exists()) {
            throw new IllegalArgumentException(fileName + " file already exists: " + file.getAbsolutePath());
        }
        return file;
    }
}
