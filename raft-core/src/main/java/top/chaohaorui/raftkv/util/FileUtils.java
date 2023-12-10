package top.chaohaorui.raftkv.util;

import com.google.protobuf.Message;
import top.chaohaorui.raftkv.proto.RaftProto;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.zip.CRC32;

public class FileUtils {
    public static void deleteDirectory(File tmpDir) {
        if (tmpDir.isDirectory()) {
            File[] files = tmpDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        tmpDir.delete();
    }

    public static void moveDirectory(File srcDir, File desDir) {
        if (srcDir.isDirectory()) {
            if (!desDir.exists()) {
                desDir.mkdirs();
            }
            File[] files = srcDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    moveDirectory(file, new File(desDir, file.getName()));
                }
            }
        } else {
            srcDir.renameTo(desDir);
        }
    }

    public static <T extends Message> void writeProtoToFile(RandomAccessFile randomAccessFile, T proto) throws IOException {
        byte[] data = proto.toByteArray();
        long crc32 = getCRC32(data);
        int size = data.length;
        randomAccessFile.writeLong(crc32);
        randomAccessFile.writeInt(size);
        randomAccessFile.write(data);

    }

    public static <T> T readProtoFromFile(RandomAccessFile metaFile, Class<T> clazz) throws IOException {
        if(metaFile.length() == 0) return null;
        long crc32 = metaFile.readLong();
        int size = metaFile.readInt();
        if(metaFile.length() - metaFile.getFilePointer() < size){
            throw new IOException("file size error");
        }
        byte[] data = new byte[size];
        metaFile.read(data);
        long crc32FromData = getCRC32(data);
        if(crc32 != crc32FromData){
            throw new IOException("crc32 check failed");
        }
        T message = null;
        try {
            Method parseFrom = clazz.getMethod("parseFrom", byte[].class);
            message = (T)parseFrom.invoke(clazz, data);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException("class do not have parseFrom method " + clazz.getName());
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return message;
    }

    private static long getCRC32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();

    }
}
