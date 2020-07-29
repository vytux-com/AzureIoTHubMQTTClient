//
// Created by Andri Yadi on 10/29/16.
//

#include "AzureIoTHubMQTTClient.h"

AzureIoTHubMQTTClient::AzureIoTHubMQTTClient(Client& c, String iotHubHostName, String deviceId, String deviceKey):
        iotHubHostName_(iotHubHostName), deviceId_(deviceId), deviceKey_(deviceKey), PubSubClient(c, iotHubHostName, AZURE_IOTHUB_MQTT_PORT) {

    mqttCommandPublishTopic_ = "devices/" + deviceId + "/messages/events/";
}

void AzureIoTHubMQTTClient::run() {
    if (!connected()) {
        doConnect();
    }

    PubSubClient::loop();
}

void AzureIoTHubMQTTClient::end() {
    disconnect();
}

bool AzureIoTHubMQTTClient::doConnect() {
    changeEventTo(AzureIoTHubMQTTClientEventConnecting);

    auto mqttUname = iotHubHostName_ + "/" + deviceId_ + "/api-version=2016-11-14";

    auto conn = MQTT::Connect(deviceId_)
        .set_auth(mqttUname, deviceKey_)
        .set_keepalive(10);

    if (PubSubClient::connect(conn)) {
        changeEventTo(AzureIoTHubMQTTClientEventConnected);

        auto cb = [=](const MQTT::Publish& p){
            _onActualMqttMessageCallback(p);
        };

        set_callback(cb);

        //Directly subscribe
        PubSubClient::subscribe("devices/" + deviceId_ + "/messages/devicebound/#");
        return true;

    }
    else {
        DEBUGLOG("Failed to connect to Azure IoT Hub\n");
        return false;
    }
}

bool AzureIoTHubMQTTClient::sendEvent(String payload) {
    return PubSubClient::publish(MQTT::Publish(mqttCommandPublishTopic_, payload).set_qos(1).set_retain(false));
}

void AzureIoTHubMQTTClient::sendEventWithKeyVal(KeyValueMap keyValMap) {
    if (keyValMap.size() == 0) {
        return;
    }

    const int BUFFER_SIZE = JSON_OBJECT_SIZE(MAX_JSON_OBJECT_SIZE);
    StaticJsonDocument<BUFFER_SIZE> root;

    for (const auto &keyVal: keyValMap) {
        root[keyVal.first] = keyVal.second;
    }

    String jsonStr;
    serializeJson(root, jsonStr);
    DEBUGLOG("JSON: %s\n", jsonStr.c_str());

    sendEvent(jsonStr);
}

void AzureIoTHubMQTTClient::_onActualMqttMessageCallback(const MQTT::Publish &msg) {
    //Process message
    if (msg.payload_len() > 0 && parseMessageAsJson_ && commandsHandlerMap_.size() > 0) {
        StaticJsonDocument<200> jsonDoc;
        auto error = deserializeJson(jsonDoc, (char*)msg.payload(), 3);
        if (error) {
            Serial.print(F("deserializeJson() failed with code "));
            Serial.println(error.c_str());
            return;
        } else {
            String key = "";

            if (jsonDoc.containsKey("Name"))
                key = "Name";
            else if (jsonDoc.containsKey("name"))
                key = "name";

            if (!key.equals("")) {
                String cmdName = String(jsonDoc[key].as<char*>());

                for (const auto &myPair : commandsHandlerMap_) {
                    if (!cmdName.equals(myPair.first))
                        continue;

                    DEBUGLOG("Found %s command\n", cmdName.c_str());
//                    AzureIoTHubMQTTClientCommandCallback cb = myPair.second;
//                    cb(myPair.first, jsonDoc);
                }
            }
        }
    }

    // Last resort
    if (onSubscribeCallback_)
        onSubscribeCallback_(msg);
}

void AzureIoTHubMQTTClient::onCloudCommand(String command,
                                           AzureIoTHubMQTTClient::AzureIoTHubMQTTClientCommandCallback callback) {
    parseMessageAsJson_ = true;
    commandsHandlerMap_[command] = callback;
}

void AzureIoTHubMQTTClient::changeEventTo(AzureIoTHubMQTTClient::AzureIoTHubMQTTClientEvent event) {
    currentEvent_ = event;

    if (eventCallback_) {
        eventCallback_(event);
    }
}