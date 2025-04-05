package kafka.common;
import kafka.protocol.io.DataOutput;

public interface Body {
    default void serialize(DataOutput out) {}
}
