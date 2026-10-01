#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <Update.h>
#include <Wire.h>
#include <Adafruit_ADS1X15.h>
#include <IoT_inferencing.h>

#define WIFI_SSID "Admin"
#define WIFI_PASS "12345678"

#define MQTT_BROKER "84aec46d41534b009368ec20fc74362c.s1.eu.hivemq.cloud"
#define MQTT_PORT 8883
#define MQTT_USER "hardware_side"
#define MQTT_PASS "hardware_side"

WiFiClientSecure espClient;
PubSubClient client(espClient);
Adafruit_ADS1115 ads;

static float features[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE];

bool ota_started = false;
bool ota_completed = false;
bool pending_ack = false;
unsigned long last_ota_chunk_time = 0;

void callback(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, "esp32/cmd/sampling") == 0) {
    int16_t adc0 = ads.readADC_SingleEnded(0);
    float volts = ads.computeVolts(adc0);
    float norm = volts / 3.3f;

    if (norm < 0.0f) {
      norm = 0.0f;
    } else if (norm > 1.0f) {
      norm = 1.0f;
    }

    float raw_channels[10];
    for (int i = 0; i < 10; i++) {
      raw_channels[i] = norm;
    }

    for (size_t i = 0; i < EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE; i++) {
      features[i] = raw_channels[i % 10];
    }

    signal_t signal;
    int err = numpy::signal_from_buffer(features, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, &signal);
    if (err != 0) {
      return;
    }

    ei_impulse_result_t result = { 0 };
    EI_IMPULSE_ERROR r = run_classifier(&signal, &result, false);
    if (r != EI_IMPULSE_OK) {
      return;
    }

    float max_confidence = 0.0f;
    const char* best_label = "Unknown";
    for (size_t ix = 0; ix < EI_CLASSIFIER_LABEL_COUNT; ix++) {
      if (result.classification[ix].value > max_confidence) {
        max_confidence = result.classification[ix].value;
        best_label = result.classification[ix].label;
      }
    }

    int inference_time = result.timing.dsp + result.timing.classification;

    char raw_data_str[160];
    snprintf(raw_data_str, sizeof(raw_data_str),
             "[%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f]",
             raw_channels[0], raw_channels[1], raw_channels[2], raw_channels[3], raw_channels[4],
             raw_channels[5], raw_channels[6], raw_channels[7], raw_channels[8], raw_channels[9]);

    char response[512];
    snprintf(response, sizeof(response),
             "{\"class\":\"%s\",\"confidence\":%.2f,\"inference_time_ms\":%d,\"raw_data\":%s}",
             best_label, max_confidence, inference_time, raw_data_str);

    client.publish("esp32/data/result", response);
    return;
  }

  if (strcmp(topic, "esp32/ota/payload") == 0) {
    if (!ota_started) {
      if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
        return;
      }
      ota_started = true;
    }

    size_t written = Update.write(payload, length);
    if (written > 0) {
      pending_ack = true;
      last_ota_chunk_time = millis();
    }

    if (length < 1024) {
      if (Update.end(true)) {
        ota_completed = true;
      }
    }
  }
}

void reconnect() {
  while (!client.connected()) {
    String clientId = "ESP32S3_Client_" + String(random(0xffff), HEX);
    if (client.connect(clientId.c_str(), MQTT_USER, MQTT_PASS)) {
      client.subscribe("esp32/cmd/sampling", 0);
      client.subscribe("esp32/ota/payload", 0);
    } else {
      delay(2000);
    }
  }
}

void setup() {
  Serial.begin(115200);

  Wire.begin();
  ads.setGain(GAIN_ONE);
  ads.setDataRate(RATE_ADS1115_860SPS);
  ads.begin();

  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
  }

  espClient.setInsecure();
  client.setServer(MQTT_BROKER, MQTT_PORT);
  client.setCallback(callback);
  client.setBufferSize(1400);
  client.setKeepAlive(60);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    WiFi.reconnect();
    delay(1000);
    return;
  }

  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  if (pending_ack) {
    if (client.publish("esp32/ota/ack", "ACK")) {
      pending_ack = false;
    }
  }

  if (ota_started && !ota_completed && (millis() - last_ota_chunk_time > 30000)) {
    Update.abort();
    ota_started = false;
  }

  if (ota_completed) {
    client.publish("esp32/ota/ack", "ACK");
    delay(1500);
    ESP.restart();
  }

  delay(2);
}