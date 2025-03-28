package kafka.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class Utils {

    public static short fromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static byte[] toByteArray(short number) {
        return ByteBuffer.allocate(2).putShort(number).array();
    }

    public static byte[] toByteArray(int number) {
        return ByteBuffer.allocate(4).putInt(number).array();
    }

    public static Integer fromByteArrayInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * Read a UTF8 string from a byte buffer at a given offset. Note that the position of the byte buffer
     * is not affected by this method.
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position in the buffer
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray())
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length, StandardCharsets.UTF_8);
        else
            return utf8(toArray(buffer, offset, length));
    }

    /**
     * Turn the given UTF8 byte array into a string
     *
     * @param bytes The byte array
     * @return The string
     */
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position of the buffer
     * @param size The number of bytes to read into the array
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Read a buffer into a Byte array for the given offset and length
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Reads bytes from a source buffer and returns a new buffer.
     * <p> The content of the new buffer will start at this buffer's current
     * position.  Changes to this buffer's content will be visible in the new
     * buffer, and vice versa; the two buffers' position, limit, and mark
     * values will be independent.
     *
     * <p> The new buffer's position will be zero, its limit will be the number of bytes
     * read i.e. <code>bytesToRead</code>, it's capacity will be the number of bytes remaining in
     * source buffer , its mark will be undefined, and its byte order will be {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN}.
     *
     * <p>Since JDK 13, this method could be replaced with slice(int index, int length).
     *
     * @param srcBuf Source buffer where data is read from
     * @param bytesToRead Number of bytes to read
     * @return Destination buffer or null if bytesToRead is < 0
     *
     * @see ByteBuffer#slice()
     */
    public static ByteBuffer readBytes(ByteBuffer srcBuf, int bytesToRead) {
        if (bytesToRead < 0)
            return null;

        final ByteBuffer dstBuf = srcBuf.slice();
        dstBuf.limit(bytesToRead);
        srcBuf.position(srcBuf.position() + bytesToRead);

        return dstBuf;
    }

    /**
     * Read the given byte buffer into a Byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * The implementation is based on Netty's decoding of varint.
     * @see <a href="https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73">Netty's varint decoding</a>
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buffer.get()) << 28;
                        if (tmp < 0) {
                            throw new IllegalArgumentException(String.valueOf(result));
                        }
                    }
                }
            }
            return result;
        }
    }

}
