package com.signify.hue.flutterreactiveble

import android.content.Context
import com.signify.hue.flutterreactiveble.ble.RequestConnectionPriorityFailed
import com.signify.hue.flutterreactiveble.channelhandlers.BleStatusHandler
import com.signify.hue.flutterreactiveble.channelhandlers.CharNotificationHandler
import com.signify.hue.flutterreactiveble.channelhandlers.DeviceConnectionHandler
import com.signify.hue.flutterreactiveble.channelhandlers.ScanDevicesHandler
import com.signify.hue.flutterreactiveble.converters.ProtobufMessageConverter
import com.signify.hue.flutterreactiveble.converters.UuidConverter
import com.signify.hue.flutterreactiveble.model.ClearGattCacheErrorType
import com.signify.hue.flutterreactiveble.utils.discard
import com.signify.hue.flutterreactiveble.utils.toConnectionPriority
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel.Result
import io.reactivex.Single
import io.reactivex.android.schedulers.AndroidSchedulers
import java.util.UUID
import com.signify.hue.flutterreactiveble.ProtobufModel as pb

@Suppress("TooManyFunctions")
class PluginController {
    private val pluginMethods =
        mapOf<String, (call: MethodCall, result: Result) -> Unit>(
            "initialize" to this::initializeClient,
            "deinitialize" to this::deinitializeClient,
            "scanForDevices" to this::scanForDevices,
            "connectToDevice" to this::connectToDevice,
            "clearGattCache" to this::clearGattCache,
            "disconnectFromDevice" to this::disconnectFromDevice,
            "readCharacteristic" to this::readCharacteristic,
            "writeCharacteristicWithResponse" to this::writeCharacteristicWithResponse,
            "writeCharacteristicWithoutResponse" to this::writeCharacteristicWithoutResponse,
            "readNotifications" to this::readNotifications,
            "stopNotifications" to this::stopNotifications,
            "negotiateMtuSize" to this::negotiateMtuSize,
            "requestConnectionPriority" to this::requestConnectionPriority,
            "discoverServices" to this::discoverServices,
            "getDiscoveredServices" to this::discoverServices,
            "readRssi" to this::readRssi,
            "negotiateHandle" to this::negotiateHandle,
        )

    private lateinit var bleClient: com.signify.hue.flutterreactiveble.ble.BleClient

    private lateinit var scanchannel: EventChannel
    private lateinit var deviceConnectionChannel: EventChannel
    private lateinit var charNotificationChannel: EventChannel

    private lateinit var scanDevicesHandler: ScanDevicesHandler
    private lateinit var deviceConnectionHandler: DeviceConnectionHandler
    private lateinit var charNotificationHandler: CharNotificationHandler

    private val uuidConverter = UuidConverter()
    private val protoConverter = ProtobufMessageConverter()

    internal fun initialize(
        messenger: BinaryMessenger,
        context: Context,
    ) {
        bleClient = com.signify.hue.flutterreactiveble.ble.ReactiveBleClient(context)

        scanchannel = EventChannel(messenger, "flutter_reactive_ble_scan")
        deviceConnectionChannel = EventChannel(messenger, "flutter_reactive_ble_connected_device")
        charNotificationChannel = EventChannel(messenger, "flutter_reactive_ble_char_update")
        val bleStatusChannel = EventChannel(messenger, "flutter_reactive_ble_status")

        scanDevicesHandler = ScanDevicesHandler(bleClient)
        deviceConnectionHandler = DeviceConnectionHandler(bleClient)
        charNotificationHandler = CharNotificationHandler(bleClient)
        val bleStatusHandler = BleStatusHandler(bleClient)

        scanchannel.setStreamHandler(scanDevicesHandler)
        deviceConnectionChannel.setStreamHandler(deviceConnectionHandler)
        charNotificationChannel.setStreamHandler(charNotificationHandler)
        bleStatusChannel.setStreamHandler(bleStatusHandler)
    }

    internal fun deinitialize() {
        scanDevicesHandler.stopDeviceScan()
        deviceConnectionHandler.disconnectAll()
    }

    internal fun execute(
        call: MethodCall,
        result: Result,
    ) {
        pluginMethods[call.method]?.invoke(call, result) ?: result.notImplemented()
    }

    private fun initializeClient(
        call: MethodCall,
        result: Result,
    ) {
        bleClient.initializeClient()
        result.success(null)
    }

    private fun deinitializeClient(
        call: MethodCall,
        result: Result,
    ) {
        deinitialize()
        result.success(null)
    }

    private fun scanForDevices(
        call: MethodCall,
        result: Result,
    ) {
        scanDevicesHandler.prepareScan(pb.ScanForDevicesRequest.parseFrom(call.arguments as ByteArray))
        result.success(null)
    }

    private fun connectToDevice(
        call: MethodCall,
        result: Result,
    ) {
        result.success(null)
        val connectDeviceMessage = pb.ConnectToDeviceRequest.parseFrom(call.arguments as ByteArray)
        deviceConnectionHandler.connectToDevice(connectDeviceMessage)
    }

    private fun clearGattCache(
        call: MethodCall,
        result: Result,
    ) {
        val args = pb.ClearGattCacheRequest.parseFrom(call.arguments as ByteArray)
        bleClient.clearGattCache(args.deviceId)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                {
                    val info = pb.ClearGattCacheInfo.getDefaultInstance()
                    result.success(info.toByteArray())
                },
                {
                    val info =
                        protoConverter.convertClearGattCacheError(
                            ClearGattCacheErrorType.UNKNOWN,
                            it.message,
                        )
                    result.success(info.toByteArray())
                },
            )
            .discard()
    }

    private fun disconnectFromDevice(
        call: MethodCall,
        result: Result,
    ) {
        result.success(null)
        val connectDeviceMessage = pb.DisconnectFromDeviceRequest.parseFrom(call.arguments as ByteArray)
        deviceConnectionHandler.disconnectDevice(connectDeviceMessage.deviceId)
    }

    private fun readCharacteristic(
        call: MethodCall,
        result: Result,
    ) {
        result.success(null)

        val readCharMessage = pb.ReadCharacteristicRequest.parseFrom(call.arguments as ByteArray)
        val deviceId = readCharMessage.characteristic.deviceId
        val characteristic = uuidConverter.uuidFromByteArray(readCharMessage.characteristic.characteristicUuid.data.toByteArray())
        val characteristicInstance = readCharMessage.characteristic.characteristicInstanceId.toInt()

        bleClient.readCharacteristic(
            deviceId,
            characteristic,
            characteristicInstance,
        )
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { charResult ->
                    when (charResult) {
                        is com.signify.hue.flutterreactiveble.ble.CharOperationSuccessful -> {
                            val charInfo =
                                protoConverter.convertCharacteristicInfo(
                                    readCharMessage.characteristic,
                                    charResult.value.toByteArray(),
                                )
                            charNotificationHandler.addSingleReadToStream(charInfo)
                        }
                        is com.signify.hue.flutterreactiveble.ble.CharOperationFailed -> {
                            protoConverter.convertCharacteristicError(
                                readCharMessage.characteristic,
                                "Failed to connect",
                            )
                            charNotificationHandler.addSingleErrorToStream(
                                readCharMessage.characteristic,
                                charResult.errorMessage,
                            )
                        }
                    }
                },
                { throwable ->
                    protoConverter.convertCharacteristicError(
                        readCharMessage.characteristic,
                        throwable.message,
                    )
                    charNotificationHandler.addSingleErrorToStream(
                        readCharMessage.characteristic,
                        throwable?.message ?: "Failure",
                    )
                },
            )
            .discard()
    }

    private fun writeCharacteristicWithResponse(
        call: MethodCall,
        result: Result,
    ) {
        executeWriteAndPropagateResultToChannel(
            call,
            result,
            com.signify.hue.flutterreactiveble.ble.BleClient::writeCharacteristicWithResponse,
        )
    }

    private fun writeCharacteristicWithoutResponse(
        call: MethodCall,
        result: Result,
    ) {
        executeWriteAndPropagateResultToChannel(
            call,
            result,
            com.signify.hue.flutterreactiveble.ble.BleClient::writeCharacteristicWithoutResponse,
        )
    }

    private fun executeWriteAndPropagateResultToChannel(
        call: MethodCall,
        result: Result,
        writeOperation: com.signify.hue.flutterreactiveble.ble.BleClient.(
            deviceId: String,
            characteristic: UUID,
            characteristicInstanceId: Int,
            value: ByteArray,
        ) -> Single<com.signify.hue.flutterreactiveble.ble.CharOperationResult>,
    ) {
        val writeCharMessage = pb.WriteCharacteristicRequest.parseFrom(call.arguments as ByteArray)
        bleClient.writeOperation(
            writeCharMessage.characteristic.deviceId,
            uuidConverter.uuidFromByteArray(writeCharMessage.characteristic.characteristicUuid.data.toByteArray()),
            writeCharMessage.characteristic.characteristicInstanceId.toInt(),
            writeCharMessage.value.toByteArray(),
        )
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { operationResult ->
                    when (operationResult) {
                        is com.signify.hue.flutterreactiveble.ble.CharOperationSuccessful -> {
                            result.success(
                                protoConverter.convertWriteCharacteristicInfo(
                                    writeCharMessage,
                                    null,
                                ).toByteArray(),
                            )
                        }
                        is com.signify.hue.flutterreactiveble.ble.CharOperationFailed -> {
                            result.success(
                                protoConverter.convertWriteCharacteristicInfo(
                                    writeCharMessage,
                                    operationResult.errorMessage,
                                ).toByteArray(),
                            )
                        }
                    }
                },
                { throwable ->
                    result.success(
                        protoConverter.convertWriteCharacteristicInfo(
                            writeCharMessage,
                            throwable.message,
                        ).toByteArray(),
                    )
                },
            )
            .discard()
    }

    private fun readNotifications(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.NotifyCharacteristicRequest.parseFrom(call.arguments as ByteArray)
        charNotificationHandler.subscribeToNotifications(request)
        result.success(null)
    }

    private fun stopNotifications(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.NotifyNoMoreCharacteristicRequest.parseFrom(call.arguments as ByteArray)
        charNotificationHandler.unsubscribeFromNotifications(request)
        result.success(null)
    }

    private fun negotiateMtuSize(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.NegotiateMtuRequest.parseFrom(call.arguments as ByteArray)
        bleClient.negotiateMtuSize(request.deviceId, request.mtuSize)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { mtuResult ->
                    result.success(protoConverter.convertNegotiateMtuInfo(mtuResult).toByteArray())
                },
                { throwable ->
                    result.success(
                        protoConverter.convertNegotiateMtuInfo(
                            com.signify.hue.flutterreactiveble.ble.MtuNegotiateFailed(
                                request.deviceId,
                                throwable.message ?: "",
                            ),
                        ).toByteArray(),
                    )
                },
            )
            .discard()
    }

    private fun requestConnectionPriority(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.ChangeConnectionPriorityRequest.parseFrom(call.arguments as ByteArray)

        bleClient.requestConnectionPriority(request.deviceId, request.priority.toConnectionPriority())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { requestResult ->
                    result.success(
                        protoConverter
                            .convertRequestConnectionPriorityInfo(requestResult).toByteArray(),
                    )
                },
                { throwable ->
                    result.success(
                        protoConverter.convertRequestConnectionPriorityInfo(
                            RequestConnectionPriorityFailed(
                                request.deviceId,
                                throwable?.message
                                    ?: "Unknown error",
                            ),
                        ).toByteArray(),
                    )
                },
            )
            .discard()
    }

    private fun discoverServices(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.DiscoverServicesRequest.parseFrom(call.arguments as ByteArray)

        bleClient.discoverServices(request.deviceId)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe({ discoverResult ->
                result.success(protoConverter.convertDiscoverServicesInfo(request.deviceId, discoverResult).toByteArray())
            }, {
                    throwable ->
                result.error("service_discovery_failure", throwable.toString(), throwable.stackTrace.toList().toString())
            })
            .discard()
    }

    private fun readRssi(
        call: MethodCall,
        result: Result,
    ) {
        val args = pb.ReadRssiRequest.parseFrom(call.arguments as ByteArray)

        bleClient.readRssi(args.deviceId)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe({ rssi ->
                val info = protoConverter.convertReadRssiResult(rssi)
                result.success(info.toByteArray())
            }, { error ->
                result.error("read_rssi_error", error.message, null)
            })
            .discard()
    }

    private fun negotiateHandle(
        call: MethodCall,
        result: Result,
    ) {
        val request = pb.ReadCharacteristicRequest.parseFrom(call.arguments as ByteArray)
        bleClient.negotiateHandle(
            request.characteristic.deviceId,
            uuidConverter.uuidFromByteArray(
                request.characteristic.characteristicUuid.data.toByteArray(),
            ),
            request.characteristic.characteristicInstanceId.toInt(),
        )
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { handle ->
                    val bytes = ByteArray(4)
                    bytes[0] = (handle shr 24).toByte()
                    bytes[1] = (handle shr 16).toByte()
                    bytes[2] = (handle shr 8).toByte()
                    bytes[3] = handle.toByte()
                    result.success(bytes)
                },
                { throwable ->
                    result.error("negotiate_handle_error", throwable.message, null)
                },
            )
            .discard()
    }

    // ---- Binary data channel (flutter_reactive_ble_data) ----

    internal fun handleDataMessage(
        message: java.nio.ByteBuffer?,
        reply: io.flutter.plugin.common.BinaryMessenger.BinaryReply,
    ) {
        if (message == null || message.remaining() < 5) {
            reply.reply(errorResponse("invalid message length"))
            return
        }
        val bytes = ByteArray(message.remaining()).also { message.get(it) }
        val op = bytes[0].toInt() and 0xFF
        val handle = ((bytes[1].toInt() and 0xFF) shl 24) or
            ((bytes[2].toInt() and 0xFF) shl 16) or
            ((bytes[3].toInt() and 0xFF) shl 8) or
            (bytes[4].toInt() and 0xFF)
        val payload = bytes.copyOfRange(5, bytes.size)

        when (op) {
            0x01 -> handleBinaryWrite(handle, payload, withResponse = true, reply)
            0x02 -> handleBinaryWrite(handle, payload, withResponse = false, reply)
            0x03 -> handleBinaryRead(handle, reply)
            else -> reply.reply(errorResponse("unknown op: $op"))
        }
    }

    private fun handleBinaryWrite(
        handle: Int,
        payload: ByteArray,
        withResponse: Boolean,
        reply: io.flutter.plugin.common.BinaryMessenger.BinaryReply,
    ) {
        bleClient.writeWithHandle(handle, payload, withResponse)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { reply.reply(successResponse()) },
                { error -> reply.reply(errorResponse(error.message ?: "write failed")) },
            )
            .discard()
    }

    private fun handleBinaryRead(
        handle: Int,
        reply: io.flutter.plugin.common.BinaryMessenger.BinaryReply,
    ) {
        bleClient.readWithHandle(handle)
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { bytes ->
                    val response = ByteArray(1 + bytes.size)
                    response[0] = 0x00
                    bytes.copyInto(response, 1)
                    reply.reply(java.nio.ByteBuffer.wrap(response))
                },
                { error -> reply.reply(errorResponse(error.message ?: "read failed")) },
            )
            .discard()
    }

    private fun successResponse(): java.nio.ByteBuffer =
        java.nio.ByteBuffer.wrap(byteArrayOf(0x00))

    private fun errorResponse(message: String): java.nio.ByteBuffer {
        val msgBytes = message.toByteArray(Charsets.UTF_8)
        val buf = ByteArray(1 + msgBytes.size)
        buf[0] = 0x01
        msgBytes.copyInto(buf, 1)
        return java.nio.ByteBuffer.wrap(buf)
    }

}
