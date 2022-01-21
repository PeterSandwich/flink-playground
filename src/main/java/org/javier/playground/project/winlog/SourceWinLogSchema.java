package org.javier.playground.project.winlog;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SourceWinLogSchema implements DeserializationSchema<WinLog>, SerializationSchema<WinLog> {

    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public WinLog deserialize(byte[] message) throws IOException {
        return JSON.parseObject(message, WinLog.class);
    }

    @Override
    public boolean isEndOfStream(WinLog nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(WinLog element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<WinLog> getProducedType() {
        return TypeInformation.of(WinLog.class);
    }
}
