#include <Wire.h>
#include <Adafruit_ADS1X15.h>

Adafruit_ADS1115 ads;

unsigned long previousMillis = 0;
const unsigned long interval = 20;

void setup() {
  Serial.begin(115200);
  Wire.begin();
  ads.setGain(GAIN_ONE);
  ads.setDataRate(RATE_ADS1115_860SPS);
  ads.begin();
}

void loop() {
  unsigned long currentMillis = millis();
  if (currentMillis - previousMillis >= interval) {
    previousMillis = currentMillis;

    int16_t adc0 = ads.readADC_SingleEnded(0);
    float volts = ads.computeVolts(adc0);
    float norm = volts / 3.3f;

    if (norm < 0.0f) {
      norm = 0.0f;
    } else if (norm > 1.0f) {
      norm = 1.0f;
    }

    for (int i = 0; i < 10; i++) {
      Serial.print(norm, 4);
      if (i < 9) {
        Serial.print(',');
      }
    }
    Serial.println();
  }
}