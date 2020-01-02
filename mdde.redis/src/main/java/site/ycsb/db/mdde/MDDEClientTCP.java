package site.ycsb.db.mdde;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Simple MDDE TCP client using standard Java socket API directly.
 */
public class MDDEClientTCP implements IMDDEClient {
  private final Socket socket;
  private final DataInputStream in;
  private final DataOutputStream out;

  /**
   * Constructor.
   * @param host MDDE TCP server host.
   * @param port MDDE TCP server port.
   */
  public MDDEClientTCP(String host, int port) throws Exception {
    if(port <= 0){
      throw new IllegalArgumentException(String.format("Invalid port value: %d", port));
    }
    if(host == null || host.isEmpty()){
      throw new IllegalArgumentException("Host value can't be null or empty");
    }
    socket = new Socket(host, port);
    in = new DataInputStream(socket.getInputStream());
    out = new DataOutputStream(socket.getOutputStream());
  }

  @Override
  public String sendCommand(String command) throws IOException {
    byte[] message = command.getBytes(StandardCharsets.UTF_8);
    byte[] length = intToByteArray(message.length);
    byte[] bArr = new byte[4 + message.length];
    System.arraycopy(message, 0, bArr, 4, message.length);
    System.arraycopy(length, 0, bArr, 0, length.length);

    out.write(bArr, 0, bArr.length);
    out.flush();
    int lenFieldLength = 4; // Length of the length part of the response frame
    byte[] responseLength = new byte[lenFieldLength];
    for(int i = 0; i < lenFieldLength; i++){
      responseLength[i] = in.readByte();
    }
    int parsedLength = byteArrayToInt(responseLength);
    byte[] responsePayload = new byte[parsedLength];

    boolean gotFullResponse = false;
    int bytesRead = 0;
    while(!gotFullResponse){
      bytesRead += in.read(responsePayload);
      if (bytesRead == parsedLength){
        gotFullResponse = true;
      }
    }
    return new String(responsePayload, StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws Exception {
    if(socket != null){
      socket.close();
    }
  }

  /**
   * Covert a big-endian 4 byte array to the integer value.
   * @param bytes 4 byte array containing an integer value.
   * @return Integer value.
   */
  private int byteArrayToInt(byte[] bytes) {
    if(bytes.length != 4){
      throw new IllegalArgumentException(
          String.format("Expected a byte array of length 4 but received: %d", bytes.length));
    }

    return bytes[3] & 0xFF |
          (bytes[2] & 0xFF) << 8 |
          (bytes[1] & 0xFF) << 16 |
          (bytes[0] & 0xFF) << 24;
  }

  /**
   * Convert Integer to a 4 byte big-endian array.
   * @param number Integer for conversion.
   * @return array of 4 bytes.
   */
  private byte[] intToByteArray(int number) {
    return new byte[] {
        (byte) ((number >> 24) & 0xFF),
        (byte) ((number >> 16) & 0xFF),
        (byte) ((number >> 8) & 0xFF),
        (byte) (number & 0xFF)
    };
  }
}
