package com.signify.hue.flutterreactiveble.channelhandlers

import com.polidea.rxandroidble2.exceptions.BleDisconnectedException
import com.signify.hue.flutterreactiveble.converters.UuidConverter
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.EventChannel
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.disposables.Disposable
import com.signify.hue.flutterreactiveble.ProtobufModel as pb

class CharNotificationHandler(
    private val bleClient: com.signify.hue.flutterreactiveble.ble.BleClient,
    private val binaryMessenger: BinaryMessenger,
) : EventChannel.StreamHandler {
    private val uuidConverter = UuidConverter()

    companion object {
        private const val CHAR_UPDATE_CHANNEL = "flutter_reactive_ble_char_update_binary"
        private val subscriptionMap = mutableMapOf<pb.CharacteristicAddress, Disposable>()
    }

    override fun onListen(objectSink: Any?, eventSink: EventChannel.EventSink?) {}

    override fun onCancel(objectSink: Any?) {
        unsubscribeFromAllNotifications()
    }

    fun subscribeToNotifications(request: pb.NotifyCharacteristicRequest) {
        val charUuid = uuidConverter.uuidFromByteArray(
            request.characteristic.characteristicUuid.data.toByteArray()
        )
        val instanceId = request.characteristic.characteristicInstanceId.toInt()
        val deviceId = request.characteristic.deviceId

        val client = bleClient as? com.signify.hue.flutterreactiveble.ble.ReactiveBleClient
        val existingHandle = client?.getHandleForChar(deviceId, charUuid, instanceId)

        val handleSingle = if (existingHandle != null) {
            io.reactivex.Single.just(existingHandle)
        } else {
            bleClient.negotiateHandle(deviceId, charUuid, instanceId)
        }

        val subscription = handleSingle
            .flatMapObservable { handle ->
                bleClient.setupNotification(deviceId, charUuid, instanceId)
                    .observeOn(AndroidSchedulers.mainThread())
                    .doOnNext { value -> handleNotificationValue(handle, value) }
            }
            .subscribe(
                { /* handled in doOnNext */ },
                { error ->
                    when (error) {
                        is BleDisconnectedException ->
                            subscriptionMap.remove(request.characteristic)?.dispose()
                        else ->
                            handleNotificationError(request.characteristic, error)
                    }
                }
            )
        subscriptionMap[request.characteristic] = subscription
    }

    fun unsubscribeFromNotifications(request: pb.NotifyNoMoreCharacteristicRequest) {
        subscriptionMap.remove(request.characteristic)?.dispose()
    }

    private fun unsubscribeFromAllNotifications() {
        subscriptionMap.forEach { it.value.dispose() }
        subscriptionMap.clear()
    }

    private fun handleNotificationValue(handle: Int, value: ByteArray) {
        val encoded = ByteArray(4 + value.size)
        encoded[0] = (handle shr 24).toByte()
        encoded[1] = (handle shr 16).toByte()
        encoded[2] = (handle shr 8).toByte()
        encoded[3] = handle.toByte()
        value.copyInto(encoded, 4)
        binaryMessenger.send(CHAR_UPDATE_CHANNEL, java.nio.ByteBuffer.wrap(encoded), null)
    }

    private fun handleNotificationError(
        address: pb.CharacteristicAddress,
        error: Throwable,
    ) {
        val msgBytes = (error.message ?: "notification error").toByteArray(Charsets.UTF_8)
        val encoded = ByteArray(4 + 1 + msgBytes.size)
        // handle = 0 (unknown), status byte = 0x01
        encoded[4] = 0x01
        msgBytes.copyInto(encoded, 5)
        binaryMessenger.send(CHAR_UPDATE_CHANNEL, java.nio.ByteBuffer.wrap(encoded), null)
    }
}
