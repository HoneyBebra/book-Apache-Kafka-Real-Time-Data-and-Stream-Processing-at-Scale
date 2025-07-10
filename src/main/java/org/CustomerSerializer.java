package org;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Ничего не настраиваем
    }

    @Override
    /**
    Сериализуем Customer как:
    4-байтный int - customerID
    4-байтный int - длина customerName в байтах в кодировке UTF-8 (0, если имя не заполнено)
    N байт - customerName в кодировке UTF-8 (0, если имя не заполнено)
    **/
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializedName;
            int stringSize;
            if (data == null) {
                return null;
            }
            else if (data.getName() != null) {
                serializedName = data.getName().getBytes("UTF-8");
                stringSize = serializedName.length;
            }
            else {
                serializedName = new byte[0];
                stringSize = 0;
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public void close() {
        // Ничего не закрываем
    }
}
