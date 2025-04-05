package kafka.protocol.io;

import lombok.SneakyThrows;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStream implements DataOutput, AutoCloseable {

    private final java.io.DataOutputStream delegate;

    public DataOutputStream(OutputStream outputStream) {
        this.delegate = new java.io.DataOutputStream(outputStream);
    }

//    @SneakyThrows
    @Override
    public void writeRawBytes(byte[] bytes) {
        try {
            delegate.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public void writeByte(byte value) {
        try {
            delegate.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public void writeShort(short value) {
        try {
            delegate.writeShort(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public void writeInt(int value) {
        try {
            delegate.writeInt(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public void writeLong(long value) {
        try {
            delegate.writeLong(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public void close() {
        try {
            delegate.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
