//! Konfigurasi LOKAL. JANGAN dibagikan atau di-commit.
//! (Kalau memakai git, tambahkan `src/config.rs` ke .gitignore.)

use core::ffi::CStr;

/// WiFi 2.4 GHz (ESP32-S3 tidak mendukung 5 GHz).
pub const WIFI_SSID: &str = "Ahmadin";
pub const WIFI_PASS: &str = "ayamgoreng";

/// Alamat broker HiveMQ Cloud, TANPA "mqtts://" dan TANPA ":8883".
/// Ditulis sebagai C-string (c"...") karena dipakai juga sebagai SNI TLS.
pub const MQTT_HOST: &CStr = c"84aec46d41534b009368ec20fc74362c.s1.eu.hivemq.cloud";
pub const MQTT_PORT: u16 = 8883;

pub const MQTT_USER: &str = "hardware_side";
pub const MQTT_PASS: &str = "hardware_side";

/// ID klien harus unik per koneksi di broker.
pub const MQTT_CLIENT_ID: &str = "esp32s3_ota_blink";

/// Topik status "masih hidup".
pub const TOPIC_STATUS: &str = "esp32/status";

// ---------------------------------------------------------------------------
// ThingsBoard (koneksi KEDUA, paralel dengan HiveMQ di atas). Plaintext MQTT
// (bukan TLS), sesuai firmware lama (enose_nostd).
// ---------------------------------------------------------------------------
pub const THINGSBOARD_HOST: &str = "thingsboard.cloud";
pub const THINGSBOARD_PORT: u16 = 1883;
pub const THINGSBOARD_ACCESS_TOKEN: &str = "9et48E0m7LHgiLal2xw9";
pub const THINGSBOARD_TOPIC: &str = "v1/devices/me/telemetry";
