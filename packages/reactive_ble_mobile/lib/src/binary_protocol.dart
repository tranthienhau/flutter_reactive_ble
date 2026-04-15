import 'dart:convert';
import 'dart:typed_data';

// Op codes (1 byte)
const int opWriteWithResponse    = 0x01;
const int opWriteWithoutResponse = 0x02;
const int opRead                 = 0x03;

// Status codes (1 byte)
const int statusSuccess = 0x00;
const int statusError   = 0x01;

class BleResponse {
  const BleResponse({required this.status, required this.bytes});
  final int status;
  final Uint8List bytes;
}

class BleNotification {
  const BleNotification({required this.handle, required this.payload});
  final int handle;
  final Uint8List payload;
}

/// Encode a write request: [1B op][4B handle BE][N bytes payload]
ByteData encodeRequest(int op, int handle, List<int> payload) {
  final data = ByteData(5 + payload.length)
    ..setUint8(0, op)
    ..setInt32(1, handle, Endian.big);
  for (var i = 0; i < payload.length; i++) {
    data.setUint8(5 + i, payload[i]);
  }
  return data;
}

/// Encode a read request (no payload): [1B op=0x03][4B handle BE]
ByteData encodeReadRequest(int handle) =>
    ByteData(5)
      ..setUint8(0, opRead)
      ..setInt32(1, handle, Endian.big);

/// Decode a response: [1B status][N bytes payload or error_utf8]
BleResponse decodeResponse(ByteData data) => BleResponse(
      status: data.getUint8(0),
      bytes: Uint8List.view(
        data.buffer,
        data.offsetInBytes + 1,
        data.lengthInBytes - 1,
      ),
    );

/// Decode a notification push: [4B handle BE][N bytes BLE payload]
BleNotification decodeNotification(ByteData data) => BleNotification(
      handle: data.getInt32(0, Endian.big),
      payload: Uint8List.view(
        data.buffer,
        data.offsetInBytes + 4,
        data.lengthInBytes - 4,
      ),
    );

/// Decode UTF-8 error message from response bytes
String decodeErrorMessage(Uint8List bytes) =>
    utf8.decode(bytes, allowMalformed: true);
