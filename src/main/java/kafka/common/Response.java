package kafka.common;

import kafka.protocol.io.DataOutput;

public record Response (Header header, Body body){

    public void serialize(DataOutput output) {
        header.serialize(output);
        body.serialize(output);

    }

}
