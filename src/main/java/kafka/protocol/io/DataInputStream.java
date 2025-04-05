package kafka.protocol.io;

import lombok.SneakyThrows;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputStream implements DataInput {

    private final java.io.DataInputStream delegate;

    public DataInputStream(InputStream in) {
        this.delegate = new java.io.DataInputStream(in);
    }

    public int available() throws IOException {
        return delegate.available();
    }

    @SneakyThrows
    public ByteBuffer readNBytes(int n) {
        try {
            return ByteBuffer.wrap(delegate.readNBytes(n));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public byte peekByte() {
        byte value;
        try {
            delegate.mark(1);
            value = delegate.readByte();
            delegate.reset();
            return value;
        } catch (IOException e) {
            return -1;
//            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public byte readSignedByte() {
        try {
            return delegate.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public short readSignedShort() {
        try {
            return delegate.readShort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public int readSignedInt(){
        try {
            return delegate.readInt();
        } catch (EOFException eof) {
//            System.out.println(eof.getMessage());
            return 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @SneakyThrows
    @Override
    public long readSignedLong(){
        try {
            return delegate.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
