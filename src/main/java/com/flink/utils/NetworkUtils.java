package com.flink.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NetworkUtils {
    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    // guava提供的java不可变集合, scala中就不用这么麻烦地的构建了
    private static final ImmutableMap<String, TimeUnit> timeSuffixes =
            ImmutableMap.<String, TimeUnit>builder()
                    .put("us", TimeUnit.MICROSECONDS)
                    .put("ms", TimeUnit.MILLISECONDS)
                    .put("s", TimeUnit.SECONDS)
                    .put("m", TimeUnit.MINUTES)
                    .put("min", TimeUnit.MINUTES)
                    .put("h", TimeUnit.HOURS)
                    .put("d", TimeUnit.DAYS)
                    .build();

    private static final ImmutableMap<String, ByteUnit> byteSuffixes =
            ImmutableMap.<String, ByteUnit>builder()
                    .put("b", ByteUnit.BYTE)
                    .put("k", ByteUnit.KiB)
                    .put("kb", ByteUnit.KiB)
                    .put("m", ByteUnit.MiB)
                    .put("mb", ByteUnit.MiB)
                    .put("g", ByteUnit.GiB)
                    .put("gb", ByteUnit.GiB)
                    .put("t", ByteUnit.TiB)
                    .put("tb", ByteUnit.TiB)
                    .put("p", ByteUnit.PiB)
                    .put("pb", ByteUnit.PiB)
                    .build();

    /**
     * 把时间描述字符串转成给定单位的时间数, 主要使用下面的两个: timeStringAsMs(单位转成毫秒), timeStringAsSec(单位转成秒)
     * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit.
     * The unit is also considered the default if the given string does not specify a unit.
     */
    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    /**
     * 把时间描述字符串转成毫秒: 50s => 50 * 1000(传单位), 50 => 50(不传单位)
     * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in ms.
     */
    public static long timeStringAsMs(String str) {
        return timeStringAs(str, TimeUnit.MILLISECONDS);
    }

    /**
     * 把时间描述字符串转成秒: 10min => 10 * 60(传单位), 10 => 10(不传单位)
     * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in seconds.
     */
    public static long timeStringAsSec(String str) {
        return timeStringAs(str, TimeUnit.SECONDS);
    }

    /**
     * 把字节描述字符串转成给定单位的数, 主要使用下面的四个: byteStringAsBytes(单位转成字节), byteStringAsKb(单位转成Kb),
     *                                                   byteStringAsMb(单位转成Mb), byteStringAsGb(单位转成Gb)
     * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is
     * provided, a direct conversion to the provided unit is attempted.
     */
    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);

                // Check for invalid suffixes
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                }

                // If suffix is valid use that, otherwise none was provided and use the default passed
                return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException("Fractional values are not supported. Input was: "
                        + fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }

        } catch (NumberFormatException e) {
            String byteError = "Size must be specified as bytes (b), " +
                    "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
                    "E.g. 50b, 100k, or 250m.";

            throw new NumberFormatException(byteError + "\n" + e.getMessage());
        }
    }

    /**
     * 单位转化为字节
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in bytes.
     */
    public static long byteStringAsBytes(String str) {
        return byteStringAs(str, ByteUnit.BYTE);
    }

    /**
     * 单位转化为Kb
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in kibibytes.
     */
    public static long byteStringAsKb(String str) {
        return byteStringAs(str, ByteUnit.KiB);
    }

    /**
     * 单位转化为Mb
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in mebibytes.
     */
    public static long byteStringAsMb(String str) {
        return byteStringAs(str, ByteUnit.MiB);
    }

    /**
     * 单位转化为Gb
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
     * internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in gibibytes.
     */
    public static long byteStringAsGb(String str) {
        return byteStringAs(str, ByteUnit.GiB);
    }

    /**
     * 递归地删除文件或目录, 不删除symlinks目录
     * Delete a file or directory and its contents recursively.
     * Don't follow directories if they are symlinks.
     *
     * @param file Input file / dir to be deleted
     * @throws IOException if deletion is unsuccessful
     */
    public static void deleteRecursively(File file) throws IOException {
        deleteRecursively(file, null);
    }

    /**
     * 参数filter为null则代表删除全部
     * Delete a file or directory and its contents recursively.
     * Don't follow directories if they are symlinks.
     *
     * @param file Input file / dir to be deleted
     * @param filter A filename filter that make sure only files / dirs with the satisfied filenames
     *               are deleted.
     * @throws IOException if deletion is unsuccessful
     */
    public static void deleteRecursively(File file, FilenameFilter filter) throws IOException {
        if (file == null) { return; }

        // linux系统调用系统"rm -rf"命令运行的更快, 如果失败退回Java IO方式删除
        // On Unix systems, use operating system command to run faster
        // If that does not work out, fallback to the Java IO way
        if (SystemUtils.IS_OS_UNIX && filter == null) {
            try {
                // linux系统命令调用成功直接返回
                deleteRecursivelyUsingUnixNative(file);
                return;
            } catch (IOException e) {
                logger.warn("Attempt to delete using native Unix OS command failed for path = {}. " +
                        "Falling back to Java IO way", file.getAbsolutePath(), e);
            }
        }

        deleteRecursivelyUsingJavaIO(file, filter);
    }

    private static void deleteRecursivelyUsingJavaIO(
            File file,
            FilenameFilter filter) throws IOException {
        if (file.isDirectory() && !isSymlink(file)) {
            IOException savedIOException = null;
            for (File child : listFilesSafely(file, filter)) {
                try {
                    deleteRecursively(child, filter);
                } catch (IOException e) {
                    // 多个异常, 只会抛出最后一个异常
                    // In case of multiple exceptions, only last one will be thrown
                    savedIOException = e;
                }
            }
            if (savedIOException != null) {
                throw savedIOException;
            }
        }

        // Delete file only when it's a normal file or an empty directory.
        if (file.isFile() || (file.isDirectory() && listFilesSafely(file, null).length == 0)) {
            boolean deleted = file.delete();
            // Delete can also fail if the file simply did not exist.
            if (!deleted && file.exists()) {
                throw new IOException("Failed to delete: " + file.getAbsolutePath());
            }
        }
    }

    private static void deleteRecursivelyUsingUnixNative(File file) throws IOException {
        ProcessBuilder builder = new ProcessBuilder("rm", "-rf", file.getAbsolutePath());
        Process process = null;
        int exitCode = -1;

        try {
            // In order to avoid deadlocks, consume the stdout (and stderr) of the process
            builder.redirectErrorStream(true);
            builder.redirectOutput(new File("/dev/null"));

            process = builder.start();

            exitCode = process.waitFor();
        } catch (Exception e) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath(), e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }

        if (exitCode != 0 || file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath());
        }
    }

    private static File[] listFilesSafely(File file, FilenameFilter filter) throws IOException {
        if (file.exists()) {
            File[] files = file.listFiles(filter);
            if (files == null) {
                throw new IOException("Failed to list files for dir: " + file);
            }
            return files;
        } else {
            return new File[0];
        }
    }

    private static boolean isSymlink(File file) throws IOException {
        Preconditions.checkNotNull(file);
        File fileInCanonicalDir = null;
        if (file.getParent() == null) {
            fileInCanonicalDir = file;
        } else {
            fileInCanonicalDir = new File(file.getParentFile().getCanonicalFile(), file.getName());
        }
        return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }
}
