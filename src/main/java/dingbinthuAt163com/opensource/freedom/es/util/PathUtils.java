package dingbinthuAt163com.opensource.freedom.es.util;

import java.io.File;

/**
 * @author DingBin , dingbinthu@163.com
 * @date 08 18 2016, 13:23
 */
public class PathUtils {

    /**
     * Get "xxx/xxx/xxx/target" or "xxx/xxx/classes" 目录
     */
    public static String getCurrentPath(Class<?> cls) {
        String path = cls.getProtectionDomain().getCodeSource().getLocation().getPath();
        path = path.replaceFirst("file:/", "");
        path = path.replaceAll("!/", "");
        if(path.lastIndexOf("/") >= 0) {
            path = path.substring(0, path.lastIndexOf("/"));
        }

        if(path.substring(0, 1).equalsIgnoreCase("/")) {
            String osName = System.getProperty("os.name").toLowerCase();
            if(osName.indexOf("window") >= 0) {
                path = path.substring(1);
            }
        }

        return path;
    }


    /**
     *
     * @param clz
     * @param roleName ---such as 'Resources'
     * @return absolute path name, which under 'target' or 'classes' directory
     */
    public static String getProjRoleDirPath(Class<?> clz, String roleName) {
        String curPath = PathUtils.getCurrentPath(clz);

        File tmpFile = new File(curPath + "/" + roleName);
        if (tmpFile.exists() && tmpFile.isDirectory()) {
            return tmpFile.getAbsolutePath();
        }
        else {
            tmpFile = new File(curPath + "/../"  +  roleName);
            if (tmpFile.exists() && tmpFile.isDirectory()) {
                return tmpFile.getAbsolutePath();
            }
            else {
                return null;
            }
        }
    }

    /**
     *
     * @param clz
     * @param roleName ---such as 'Resources'
     * @return absolute path name, which under 'target' or 'classes' directory
     */
    public static String getProjRoleFilePath(Class<?> clz, String roleName) {
        String curPath = PathUtils.getCurrentPath(clz);

        File tmpFile = new File(curPath + "/" + roleName);
        if (tmpFile.exists() && tmpFile.isFile()) {
            return tmpFile.getAbsolutePath();
        }
        else {
            tmpFile = new File(curPath + "/../" +  roleName);
            if (tmpFile.exists() && tmpFile.isFile()) {
                return tmpFile.getAbsolutePath();
            }
            else {
                return null;
            }
        }
    }

    public static void main(String [] args) {
        System.err.println("========getCurrentPath(PathUtils.class):" +  getCurrentPath(PathUtils.class) + "=========");
        System.err.println("========getProjRoleDirPath(PathUtils.class, resources):" +  getProjRoleDirPath(PathUtils.class,"resources") + "=========");
        System.err.println("========getProjRoleFilePath(PathUtils.class,resources):" +  getProjRoleFilePath(PathUtils.class,"resources") + "=========");

        System.err.println("========getProjRoleDirPath(PathUtils.class,log4j.properties):" +  getProjRoleDirPath(PathUtils.class,"kafka.producer.properties") + "=========");
        System.err.println("========getProjRoleFilePath(PathUtils.class,log4j.properties):" +  getProjRoleFilePath(PathUtils.class,"kafka.producer.properties") + "=========");
    }

}
