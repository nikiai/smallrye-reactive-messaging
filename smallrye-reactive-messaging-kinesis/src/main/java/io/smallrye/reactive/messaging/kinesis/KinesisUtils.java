package io.smallrye.reactive.messaging.kinesis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class KinesisUtils {

  static byte[] serializeObject(Object obj) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
    oos.writeObject(obj);
    oos.flush();
    byte[] bytes = bytesOut.toByteArray();
    bytesOut.close();
    oos.close();
    return bytes;
  }
}
