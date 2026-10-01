//! E-Nose — UJI OTA LEWAT THINGSBOARD (bukan HiveMQ). LED berkedip + DHT22 +
//! penerima OTA RESMI ThingsBoard (paket "OTA Updates" di dashboard).
//!
//! BEDA dari hive_ota_blink: di situ OTA didorong (push) lewat MQTT custom ke
//! HiveMQ. Di sini OTA ditarik (pull) lewat protokol RESMI ThingsBoard --
//! ESP32 yang minta potongan firmware satu-satu, bukan menerima kiriman.
//! HiveMQ TETAP dipakai untuk status/LED SAJA, TIDAK untuk OTA (satu firmware
//! ini sengaja cuma punya SATU jalur penulis flash aktif, supaya tidak perlu
//! kunci/sinkronisasi tambahan antara dua task yang menulis flash bersamaan).
//!
//! !!! BAGIAN PALING BERISIKO SEJAUH INI, DAN PROTOKOLNYA BELUM PERNAH DIUJI
//! DI PERANGKAT SUNGGUHAN SAMA SEKALI !!! Beda dari hive_ota_blink (yang
//! sudah terbukti jalan), jalur ThingsBoard ini seluruhnya BARU: topik MQTT-
//! nya sesuai dokumentasi resmi ThingsBoard, tapi format persis beberapa
//! detail (mis. bentuk payload permintaan potongan) belum pernah saya
//! buktikan langsung ke server sungguhan. Kalau sesuatu terpotong di tengah,
//! ESP32 TETAP menjalankan firmware lama (partisi baru diaktifkan HANYA
//! setelah SHA-256 cocok).
//!
//! Perlu tabel partisi khusus (`partitions.csv`, 3 slot app: factory + ota_0 +
//! ota_1) -- SUDAH disertakan, sama seperti hive_ota_blink.
//!
//! ALUR OTA (protokol RESMI ThingsBoard, semua lewat MQTT plaintext port 1883):
//!  1. Kamu upload firmware.bin di ThingsBoard (menu "OTA Updates"), isi
//!     Title & Version SAMA PERSIS dengan FW_TITLE/FW_VERSION di bawah, lalu
//!     assign paket itu ke device-mu. ThingsBoard otomatis menghitung SHA-256
//!     dan menyetel shared attribute fw_title/fw_version/fw_size/fw_checksum/
//!     fw_checksum_algorithm.
//!  2. ESP32 minta attribute itu (v1/devices/me/attributes/request/1), lihat
//!     fw_title/fw_version beda dari yang sedang dia jalankan -> mulai unduh.
//!  3. Tiap potongan diminta satu-satu: publish ke
//!     v2/fw/request/{requestId}/chunk/{chunkIndex}, isi payload = ukuran
//!     potongan yang diminta (byte). Jawabannya di
//!     v2/fw/response/{requestId}/chunk/{chunkIndex}, isi = data firmware
//!     mentah. chunkIndex naik terus sampai jawabannya KOSONG (= selesai).
//!  4. Tiap potongan LANGSUNG ditulis ke flash (reuse fungsi yang sama
//!     terbukti dari hive_ota_blink) sambil dihitung SHA-256 berjalan.
//!  5. Selesai -> SHA-256 final dibandingkan dengan fw_checksum (heksadesimal
//!     huruf kecil, 64 karakter). Cocok -> aktifkan partisi baru, lapor
//!     telemetry {"fw_state":"UPDATED"}, reboot. Tidak cocok -> BATAL, lapor
//!     {"fw_state":"FAILED"}, firmware lama tetap jalan.

#![no_std]
#![no_main]
#![allow(linker_messages)]

extern crate alloc;

use alloc::string::ToString;
use core::fmt::Write as _;

use embassy_executor::Spawner;
use embassy_net::dns::DnsQueryType;
use embassy_net::tcp::TcpSocket;
use embassy_net::{Config as NetConfig, IpEndpoint, Runner, StackResources};
use embassy_time::{Duration, Instant as EmbInstant, Timer, with_timeout};
use embedded_io_async_06::{
    Error as Error06, ErrorKind as ErrorKind06, ErrorType as ErrorType06, Read as Read06,
    Write as Write06,
};
use esp_backtrace as _;
use esp_hal::clock::CpuClock;
use esp_hal::delay::Delay;
use esp_hal::gpio::{Flex, Level, Output, OutputConfig};
use esp_hal::rng::{Rng, Trng, TrngSource};
use esp_hal::time::Instant;
use esp_hal::timer::timg::TimerGroup;
use esp_radio::wifi::sta::StationConfig;
use esp_radio::wifi::{Config as WifiConfig, Interface, WifiController};
use log::{info, warn};
use mbedtls_rs::{AuthMode, ClientSessionConfig, Session, SessionConfig, Tls, TlsVersion};
use rust_mqtt::client::client::MqttClient;
use rust_mqtt::client::client_config::{ClientConfig, MqttVersion};
use rust_mqtt::packet::v5::publish_packet::QualityOfService;
use rust_mqtt::utils::rng_generator::CountingRng;
use static_cell::StaticCell;

use esp_bootloader_esp_idf::ota::OtaImageState;
use esp_bootloader_esp_idf::ota_updater::OtaUpdater;
use esp_bootloader_esp_idf::partitions::PARTITION_TABLE_MAX_LEN;
use esp_storage::FlashStorage;
use sha2::{Digest, Sha256};

// Konfigurasi lokal (WiFi, host, user, password). Ada di src/config.rs.
#[path = "../config.rs"]
mod config;

esp_bootloader_esp_idf::esp_app_desc!();

// ===========================================================================
// KONFIGURASI (ubah di sini saja)
// ===========================================================================

/// >>> SATU-SATUNYA BARIS YANG DIUBAH ANTARA FIRMWARE PERTAMA DAN FIRMWARE
/// KEDUA. Flash pertama (USB) pakai 5000; sebelum dikirim lewat OTA, ganti
/// jadi 2000, build ulang, lalu kirim dengan ota_send.py. Kalau setelah OTA
/// LED berkedip 2 detik sekali (bukan 5), OTA terbukti bekerja. <<<
const BLINK_MS: u64 = 1000;

/// >>> HARUS SAMA PERSIS dengan Title & Version yang kamu isi saat upload
/// paket di menu "OTA Updates" ThingsBoard. Firmware membandingkan dua
/// angka ini dengan fw_title/fw_version yang di-assign; kalau BEDA, unduh
/// dimulai. Setelah OTA sukses, ganti FW_VERSION di sini (source firmware
/// BARU) supaya sama dengan versi yang baru kamu upload -- kalau lupa,
/// firmware baru akan mengira dirinya "belum update" dan coba unduh ulang
/// terus tiap boot. <<<
const FW_TITLE: &str = "OTA";
const FW_VERSION: &str = "1.0.0";

/// Satu potongan firmware yang diminta per permintaan (byte). ThingsBoard
/// pernah melaporkan bug kalau nilainya > 65536; 1024 jauh di bawah itu.
/// Ukuran yang KITA MINTA per potongan. Kode TIDAK bergantung pada nilai
/// ini untuk KEBENARAN data (lihat catatan di titik penerimaan potongan,
/// yang selalu pakai panjang ASLI yang diterima) -- nilai ini cuma
/// menentukan berapa BANYAK kali bolak-balik minta-jawab yang dibutuhkan.
///
/// Terukur dari uji nyata: server mengirim PERSIS sebesar yang diminta di
/// sini (bukan dilipatgandakan seperti dugaan awal yang salah).
///
/// !!! PENTING (koreksi KEDUA dari dugaan sebelumnya): bukan batas JUMLAH
/// pesan -- tiga kali percobaan SEMUA gagal di sekitar detik ke-310 sejak
/// boot, TIDAK PEDULI baru sampai potongan ke berapa. Ini batas WAKTU sesi
/// (~5 menit), dari ThingsBoard atau jaringan. Percobaan terakhir dengan
/// nilai 8192 sudah sampai 983040/986560 byte (99,6%!) sebelum kena batas
/// itu di permintaan PALING TERAKHIR -- jadi tinggal dipercepat sedikit
/// lagi. Dinaikkan ke 16384 supaya total permintaan turun ke ~61, jauh di
/// bawah yang dibutuhkan buat selesai dalam ~5 menit.
const OTA_CHUNK_MAX: usize = 16384;

/// Buffer MQTT (recv/write/max_packet_size) untuk task ThingsBoard. 2x
/// OTA_CHUNK_MAX (BUKAN karena server melipatgandakan -- sudah dikoreksi di
/// atas -- tapi jaga-jaga kalau DUA pesan numpuk terbaca sekaligus dalam
/// satu pembacaan TCP, seperti yang kemungkinan besar memicu panic
/// sebelumnya) + margin topik & header/properti MQTTv5.
const TB_MQTT_BUF: usize = OTA_CHUNK_MAX * 2 + 512;
/// Kalau ThingsBoard tidak membalas permintaan potongan selama ini, batalkan
/// unduhan (supaya tidak nyangkut selamanya kalau internet putus di tengah).
const OTA_STALL_MS: u64 = 20_000;

/// Jeda status "masih hidup" (ms), supaya kelihatan di MQTTX walau LED tidak
/// dilihat langsung.
const STATUS_PERIOD_MS: u64 = 5000;

// ===========================================================================
// JEMBATAN embedded-io-async 0.7 (TCP/TLS) -> 0.6 (rust-mqtt 0.3)
// PERSIS SAMA dengan hive_ota (disalin, bukan ditulis ulang).
// ===========================================================================

#[derive(Debug)]
struct IoError;

impl core::fmt::Display for IoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "TLS/MQTT IO error")
    }
}

impl core::error::Error for IoError {}

impl Error06 for IoError {
    fn kind(&self) -> ErrorKind06 {
        ErrorKind06::Other
    }
}

/// Membungkus apa pun yang mengimplementasikan Read/Write versi 0.7
/// (di sini: sesi TLS) supaya bisa dipakai rust-mqtt (versi 0.6).
struct Io06<'a, T>(&'a mut T);

impl<T> ErrorType06 for Io06<'_, T> {
    type Error = IoError;
}

impl<T: embedded_io_async::Read> Read06 for Io06<'_, T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        embedded_io_async::Read::read(&mut *self.0, buf)
            .await
            .map_err(|_| IoError)
    }
}

impl<T: embedded_io_async::Write> Write06 for Io06<'_, T> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        embedded_io_async::Write::write(&mut *self.0, buf)
            .await
            .map_err(|_| IoError)
    }

    async fn flush(&mut self) -> Result<(), IoError> {
        embedded_io_async::Write::flush(&mut *self.0)
            .await
            .map_err(|_| IoError)
    }
}

// ===========================================================================
// Buffer teks & parser JSON sederhana -- dipakai untuk MEMBACA respons
// attribute ThingsBoard dan MENYUSUN topik/telemetry yang dikirim.
// ===========================================================================

/// Buffer teks berukuran tetap di stack, implementasi `core::fmt::Write`
/// sehingga bisa dipakai dengan `write!`. Bila penuh, `write!` mengembalikan
/// error (tidak panik).
struct FixedBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> FixedBuf<N> {
    const fn new() -> Self {
        Self { buf: [0; N], len: 0 }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("<utf8?>")
    }
}

impl<const N: usize> core::fmt::Write for FixedBuf<N> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let b = s.as_bytes();
        if self.len + b.len() > N {
            return Err(core::fmt::Error);
        }
        self.buf[self.len..self.len + b.len()].copy_from_slice(b);
        self.len += b.len();
        Ok(())
    }
}

/// Ambil nilai mentah dari `"key": nilai` pada JSON sederhana (tanpa escape,
/// tanpa objek bersarang). String dikembalikan tanpa tanda kutip.
fn json_raw<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let mut pat: FixedBuf<40> = FixedBuf::new();
    write!(pat, "\"{}\"", key).ok()?;
    let idx = s.find(pat.as_str())?;
    let rest = s[idx + pat.len..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    if let Some(r) = rest.strip_prefix('"') {
        let end = r.find('"')?;
        Some(&r[..end])
    } else {
        let end = rest
            .find(|c: char| c == ',' || c == '}' || c.is_whitespace())
            .unwrap_or(rest.len());
        Some(&rest[..end])
    }
}

/// Bilangan bulat tak bertanda dari `"key": 300`.
fn json_u32(s: &str, key: &str) -> Option<u32> {
    let raw = json_raw(s, key)?;
    raw.parse::<u32>()
        .ok()
        .or_else(|| raw.parse::<f32>().ok().filter(|f| *f >= 0.0).map(|f| f as u32))
}

fn build_status<const N: usize>(
    out: &mut FixedBuf<N>,
    fw: &str,
    uptime_s: u64,
    heap: usize,
    blink_ms: u64,
) -> core::fmt::Result {
    write!(
        out,
        "{{\"fw\":\"{}\",\"uptime_s\":{},\"heap\":{},\"blink_ms\":{}}}",
        fw, uptime_s, heap, blink_ms
    )
}

// ===========================================================================
// OTA lewat ThingsBoard -- logika murni (parser attribute, hex, topik)
// ===========================================================================

/// Isi shared attribute yang relevan, hasil parse dari respons ThingsBoard
/// (baik dari `.../attributes/response/+` maupun push `.../attributes`).
/// String-string di sini adalah SLICE dari `s` (tidak dibuat salinan).
#[derive(Debug, PartialEq)]
struct FwAttrs<'a> {
    title: &'a str,
    version: &'a str,
    size: u32,
    checksum: &'a str,
    algorithm: &'a str,
}

/// `None` kalau salah satu dari fw_title/fw_version tidak ada di pesan ini
/// (mis. pesan attribute yang tidak berkaitan dengan firmware).
fn parse_fw_attrs(s: &str) -> Option<FwAttrs<'_>> {
    Some(FwAttrs {
        title: json_raw(s, "fw_title")?,
        version: json_raw(s, "fw_version")?,
        size: json_u32(s, "fw_size").unwrap_or(0),
        checksum: json_raw(s, "fw_checksum").unwrap_or(""),
        algorithm: json_raw(s, "fw_checksum_algorithm").unwrap_or(""),
    })
}

/// Tulis 32 byte digest sebagai heksadesimal huruf kecil (64 karakter) --
/// bentuk yang sama seperti fw_checksum dari ThingsBoard, supaya tinggal
/// dibandingkan sebagai teks (bukan didekode balik jadi byte).
fn hex_encode_32<const N: usize>(out: &mut FixedBuf<N>, digest: &[u8; 32]) -> core::fmt::Result {
    for b in digest {
        write!(out, "{:02x}", b)?;
    }
    Ok(())
}

/// {"sharedKeys":"fw_title,fw_version,fw_size,fw_checksum,fw_checksum_algorithm"}
const FW_ATTR_REQUEST: &str =
    "{\"sharedKeys\":\"fw_title,fw_version,fw_size,fw_checksum,fw_checksum_algorithm\"}";

/// v2/fw/request/{requestId}/chunk/{chunkIndex}
fn build_chunk_request_topic<const N: usize>(
    out: &mut FixedBuf<N>,
    request_id: u32,
    chunk_index: u32,
) -> core::fmt::Result {
    write!(out, "v2/fw/request/{}/chunk/{}", request_id, chunk_index)
}

/// {"current_fw_title":"..","current_fw_version":"..","fw_state":".."}
fn build_fw_telemetry<const N: usize>(
    out: &mut FixedBuf<N>,
    title: &str,
    version: &str,
    state: &str,
) -> core::fmt::Result {
    write!(
        out,
        "{{\"current_fw_title\":\"{}\",\"current_fw_version\":\"{}\",\"fw_state\":\"{}\"}}",
        title, version, state
    )
}

// ===========================================================================
// OTA — akses flash (bagian TIDAK bisa diuji di luar ESP32 sungguhan)
// ===========================================================================

fn ota_write_at<F>(
    updater: &mut OtaUpdater<'_, F>,
    offset: u32,
    data: &[u8],
) -> Result<u32, esp_bootloader_esp_idf::partitions::Error>
where
    F: embedded_storage::ReadStorage + embedded_storage::Storage,
{
    let (mut region, _slot) = updater.next_partition()?;
    let cap = region.partition_size() as u32;
    if offset.checked_add(data.len() as u32).is_none_or(|end| end > cap) {
        return Err(esp_bootloader_esp_idf::partitions::Error::OutOfBounds);
    }
    embedded_storage::Storage::write(&mut region, offset, data)?;
    Ok(cap)
}

// ===========================================================================
// MEMORI STATIS
// ===========================================================================

// <4>: dulu 3 (cukup untuk 1 soket TCP + DHCP/DNS). Dinaikkan supaya ada
// tempat untuk soket TCP KEDUA punya task ThingsBoard yang berjalan paralel.
static STACK_RESOURCES: StaticCell<StackResources<4>> = StaticCell::new();
static TCP_RX: StaticCell<[u8; 4096]> = StaticCell::new();
static TCP_TX: StaticCell<[u8; 2048]> = StaticCell::new();
static TRNG_CELL: StaticCell<Trng> = StaticCell::new();

fn heap_free() -> usize {
    esp_alloc::HEAP.free()
}

macro_rules! publish_or_break {
    ($client:expr, $topic:expr, $bytes:expr, $label:lifetime) => {
        if let Err(e) = $client
            .send_message($topic, $bytes, QualityOfService::QoS0, false)
            .await
        {
            warn!("Publish ke {} gagal: {:?}", $topic, e);
            break $label;
        }
    };
}

// ===========================================================================
// PROGRAM UTAMA
// ===========================================================================

#[esp_rtos::main]
async fn main(spawner: Spawner) -> ! {
    esp_println::logger::init_logger_from_env();

    let cfg = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(cfg);

    esp_alloc::heap_allocator!(#[esp_hal::ram(reclaimed)] size: 73744);
    esp_alloc::heap_allocator!(size: 96 * 1024);

    let timg0 = TimerGroup::new(peripherals.TIMG0);
    let sw_interrupt =
        esp_hal::interrupt::software::SoftwareInterruptControl::new(peripherals.SW_INTERRUPT);
    esp_rtos::start(timg0.timer0, sw_interrupt.software_interrupt0);

    info!("=== E-Nose UJI OTA LEWAT THINGSBOARD ({} {}) — BLINK_MS = {} ===", FW_TITLE, FW_VERSION, BLINK_MS);
    info!("Heap bebas awal: {} byte", heap_free());

    // ---------- LED luar (GPIO biasa) ----------
    // LED + resistor (220 ohm - 10k, bebas) ke GND, dari GPIO2. Opsional --
    // ada juga log "LED ON/OFF" kalau tidak pasang LED sama sekali.
    let mut led = Output::new(peripherals.GPIO2, Level::Low, OutputConfig::default());

    // ---------- Generator angka acak untuk TLS (HiveMQ) ----------
    let _trng_source = TrngSource::new(peripherals.RNG, peripherals.ADC1);
    let trng: &'static mut Trng = TRNG_CELL.init(Trng::try_new().expect("TRNG tidak siap"));
    let tls = Tls::new(trng).expect("Gagal membuat instance Tls");

    // ---------- WiFi ----------
    let rng = Rng::new();

    let (wifi_controller, interfaces) =
        esp_radio::wifi::new(peripherals.WIFI, Default::default())
            .expect("Failed to initialize Wi-Fi controller");

    let wifi_device: Interface = interfaces.station;

    let net_seed = (rng.random() as u64) << 32 | rng.random() as u64;
    let net_config = NetConfig::dhcpv4(Default::default());

    let stack_resources = STACK_RESOURCES.init(StackResources::new());
    let (stack, runner) = embassy_net::new(wifi_device, net_config, stack_resources, net_seed);

    spawner.spawn(net_task(runner).expect("Failed to spawn net_task"));
    spawner.spawn(
        wifi_connection_task(wifi_controller).expect("Failed to spawn wifi_connection_task"),
    );

    // ---------- DHT22 setup (disalin dari hive_rec/hive_ml, terbukti jalan) ----------
    let mut dht_pin = Flex::new(peripherals.GPIO4);
    dht_pin.set_input_enable(true);
    dht_pin.set_output_enable(false);
    dht_pin.set_high();
    let dht_delay = Delay::new(); // tidak perlu `mut`: dipindah (move) apa adanya ke thingsboard_task
    dht_delay.delay_millis(1000);

    // Task ThingsBoard MEMEGANG: DHT22 (baca sebelum tiap publish) DAN akses
    // flash untuk OTA (peripherals.FLASH) -- satu-satunya bagian program yang
    // boleh menulis flash, supaya tidak perlu kunci/sinkronisasi tambahan.
    spawner.spawn(
        thingsboard_task(stack, dht_pin, dht_delay, peripherals.FLASH)
            .expect("Failed to spawn thingsboard_task"),
    );

    info!("Menunggu koneksi Wi-Fi...");
    loop {
        if stack.is_link_up() {
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    info!("Link up, menunggu alamat IP (DHCP)...");
    loop {
        if let Some(c) = stack.config_v4() {
            info!("WiFi tersambung! IP: {}", c.address);
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    // ---------- DNS broker HiveMQ ----------
    let host_c = config::MQTT_HOST;
    let host = host_c.to_str().unwrap_or("");
    info!("Resolve DNS untuk {}...", host);
    let addrs = loop {
        match stack.dns_query(host, DnsQueryType::A).await {
            Ok(addrs) if !addrs.is_empty() => break addrs,
            _ => {
                info!("DNS belum berhasil, coba lagi...");
                Timer::after(Duration::from_millis(1000)).await;
            }
        }
    };
    let remote_ip = addrs[0];
    info!("Broker IP: {}", remote_ip);
    let endpoint = IpEndpoint::new(remote_ip, config::MQTT_PORT);

    let tcp_rx: &'static mut [u8; 4096] = TCP_RX.init([0; 4096]);
    let tcp_tx: &'static mut [u8; 2048] = TCP_TX.init([0; 2048]);

    let mut attempt: u32 = 0;

    // ---------- HiveMQ: status + LED SAJA (TIDAK ada OTA di jalur ini). ----------
    loop {
        attempt += 1;
        info!("--- percobaan sambung #{} (heap bebas {}) ---", attempt, heap_free());

        let mut socket = TcpSocket::new(stack, &mut tcp_rx[..], &mut tcp_tx[..]);
        info!("TCP connect ke {}:{}...", remote_ip, config::MQTT_PORT);
        if let Err(e) = socket.connect(endpoint).await {
            warn!("Gagal TCP connect: {:?}", e);
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        info!("TCP tersambung.");

        let mut tls_cfg = ClientSessionConfig::new();
        tls_cfg.server_name = Some(host_c);
        tls_cfg.auth_mode = AuthMode::None; // SEMENTARA: tanpa verifikasi sertifikat
        tls_cfg.min_version = TlsVersion::Tls1_2;
        tls_cfg.max_version = Some(TlsVersion::Tls1_2);
        let session_cfg = SessionConfig::Client(tls_cfg);

        let mut session = match Session::new(tls.reference(), &mut socket, &session_cfg) {
            Ok(s) => s,
            Err(e) => {
                warn!("Gagal membuat sesi TLS: {:?}", e);
                Timer::after(Duration::from_secs(5)).await;
                continue;
            }
        };

        let mqtt_config = {
            let mut c = ClientConfig::new(MqttVersion::MQTTv5, CountingRng(20000));
            c.add_client_id(config::MQTT_CLIENT_ID);
            c.add_username(config::MQTT_USER);
            c.add_password(config::MQTT_PASS);
            c.max_packet_size = 512;
            c
        };

        let mut recv_buffer = [0u8; 512];
        let mut write_buffer = [0u8; 512];

        let mut mqtt_client = MqttClient::<_, 5, _>::new(
            Io06(&mut session),
            &mut write_buffer,
            512,
            &mut recv_buffer,
            512,
            mqtt_config,
        );

        if let Err(e) = mqtt_client.connect_to_broker().await {
            warn!("Gagal TLS/MQTT connect: {:?}", e);
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        info!("TLS + MQTT tersambung ke HiveMQ! (heap bebas {})", heap_free());
        info!("Berkedip tiap {} ms...", BLINK_MS);

        let mut next_blink_ms: u64 = 0;
        let mut next_status_ms: u64 = 0;

        'conn: loop {
            // (a) Kedipkan LED bila sudah waktunya.
            let now_ms = EmbInstant::now().as_millis();
            if now_ms >= next_blink_ms {
                led.toggle();
                // Dicetak juga ke serial: bukti visual OTA yang tidak butuh
                // LED/resistor sama sekali. Cukup perhatikan JEDA antar baris
                // ini di log -- itulah BLINK_MS yang sedang aktif.
                info!(
                    "LED {} (t={} ms, BLINK_MS={})",
                    if led.is_set_high() { "ON" } else { "OFF" },
                    now_ms,
                    BLINK_MS
                );
                next_blink_ms = now_ms + BLINK_MS;
            }

            // (b) Status "masih hidup" berkala.
            let now_ms = EmbInstant::now().as_millis();
            if now_ms >= next_status_ms {
                let mut st: FixedBuf<128> = FixedBuf::new();
                let _ = build_status(&mut st, FW_VERSION, EmbInstant::now().as_secs(), heap_free(), BLINK_MS);
                publish_or_break!(mqtt_client, config::TOPIC_STATUS, st.as_bytes(), 'conn);
                next_status_ms = now_ms + STATUS_PERIOD_MS;
            }

            // (c) Tunggu sampai ada pekerjaan berikutnya (kedip/status).
            let now_ms = EmbInstant::now().as_millis();
            let deadline = next_blink_ms.min(next_status_ms);
            let wait_ms = deadline.saturating_sub(now_ms).max(1);

            match with_timeout(Duration::from_millis(wait_ms), mqtt_client.receive_message()).await
            {
                Ok(Ok((topic, _payload))) => {
                    info!("Pesan di topik lain diabaikan: {}", topic);
                }
                Ok(Err(e)) => {
                    warn!("Gagal menerima pesan: {:?}, sambung ulang...", e);
                    break 'conn;
                }
                Err(_) => {} // waktu habis: ada kedip/status yang jatuh tempo
            }
        }

        Timer::after(Duration::from_secs(5)).await;
    }
}

/// Task background yang menjalankan network stack (wajib selalu jalan).
#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, Interface<'static>>) -> ! {
    runner.run().await
}

/// Task background yang menjaga koneksi Wi-Fi tetap hidup (auto-reconnect).
#[embassy_executor::task]
async fn wifi_connection_task(mut controller: WifiController<'static>) {
    info!("Memulai koneksi Wi-Fi ke SSID: {}", config::WIFI_SSID);

    let station_config = StationConfig::default()
        .with_ssid(config::WIFI_SSID)
        .with_password(config::WIFI_PASS.to_string());
    let wifi_config = WifiConfig::Station(station_config);

    controller
        .set_config(&wifi_config)
        .expect("Failed to set Wi-Fi config");

    loop {
        if controller.is_connected() {
            let _ = controller.wait_for_disconnect_async().await;
            info!("Wi-Fi terputus, mencoba menyambung ulang...");
            Timer::after(Duration::from_millis(2000)).await;
        }

        match controller.connect_async().await {
            Ok(_) => info!("Wi-Fi berhasil tersambung"),
            Err(e) => {
                info!("Wi-Fi connect gagal: {:?}", e);
                Timer::after(Duration::from_millis(5000)).await;
            }
        }
    }
}

// ===========================================================================
// ThingsBoard — koneksi KEDUA (plaintext, tanpa TLS): status DHT22 + OTA RESMI
// ===========================================================================
//
// Task ini MEMEGANG SENDIRI: DHT22 (baca sebelum tiap publish, sama seperti
// hive_ota_blink) DAN akses flash untuk OTA (satu-satunya bagian program
// yang menulis flash). Berjalan independen dari loop HiveMQ di atas (soket
// TCP sendiri) -- kalau task ini berhenti, HiveMQ+LED tidak terganggu.
//
// !!! BELUM PERNAH DIUJI DI PERANGKAT SUNGGUHAN. Bagian OTA-nya (permintaan
// attribute, unduh potongan, SHA-256) sama sekali baru -- beda dari
// hive_ota_blink yang jalur HiveMQ-nya sudah terbukti. Kalau ada yang aneh,
// tempel log serial lengkapnya.

/// Batas jumlah potongan sebagai pengaman (bukan batas normal): kalau
/// ThingsBoard entah kenapa tidak pernah mengirim potongan kosong sebagai
/// tanda selesai, unduhan tetap berhenti di sini alih-alih jalan selamanya.
const OTA_MAX_CHUNKS: u32 = 1_000_000;

#[embassy_executor::task]
async fn thingsboard_task(
    stack: embassy_net::Stack<'static>,
    mut dht_pin: Flex<'static>,
    mut dht_delay: Delay,
    flash: esp_hal::peripherals::FLASH<'static>,
) -> ! {
    static TB_TCP_RX: StaticCell<[u8; 1024]> = StaticCell::new();
    static TB_TCP_TX: StaticCell<[u8; 1024]> = StaticCell::new();
    let tb_rx: &'static mut [u8; 1024] = TB_TCP_RX.init([0; 1024]);
    let tb_tx: &'static mut [u8; 1024] = TB_TCP_TX.init([0; 1024]);

    // ---------- Flash storage + OTA (dibuat SEKALI, dipakai ulang tiap sesi) ----------
    let mut flash_storage = FlashStorage::new(flash);
    static PT_BUF: StaticCell<[u8; PARTITION_TABLE_MAX_LEN]> = StaticCell::new();
    let pt_buf = PT_BUF.init([0u8; PARTITION_TABLE_MAX_LEN]);
    let mut ota_updater = match OtaUpdater::new(&mut flash_storage, pt_buf) {
        Ok(u) => {
            info!("[ThingsBoard] OTA siap (tabel partisi punya slot ota_0/ota_1).");
            Some(u)
        }
        Err(e) => {
            warn!(
                "[ThingsBoard] OTA DIMATIKAN: tabel partisi tidak punya slot OTA ({:?}). \
                 Pastikan di-flash dengan --partition-table partitions.csv.",
                e
            );
            None
        }
    };

    info!("[ThingsBoard] Menunggu WiFi...");
    loop {
        if stack.is_link_up() && stack.config_v4().is_some() {
            break;
        }
        Timer::after(Duration::from_millis(500)).await;
    }

    info!("[ThingsBoard] Resolve DNS untuk {}...", config::THINGSBOARD_HOST);
    let addrs = loop {
        match stack.dns_query(config::THINGSBOARD_HOST, DnsQueryType::A).await {
            Ok(addrs) if !addrs.is_empty() => break addrs,
            _ => {
                warn!("[ThingsBoard] DNS belum berhasil, coba lagi...");
                Timer::after(Duration::from_millis(1000)).await;
            }
        }
    };
    let remote_ip = addrs[0];
    let endpoint = IpEndpoint::new(remote_ip, config::THINGSBOARD_PORT);
    info!("[ThingsBoard] IP: {}", remote_ip);

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        info!("[ThingsBoard] --- percobaan sambung #{} ---", attempt);

        let mut socket = TcpSocket::new(stack, &mut tb_rx[..], &mut tb_tx[..]);
        if let Err(e) = socket.connect(endpoint).await {
            warn!("[ThingsBoard] Gagal TCP connect: {:?}", e);
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        info!("[ThingsBoard] TCP tersambung (plaintext, tanpa TLS).");

        let mqtt_config = {
            let mut c = ClientConfig::new(MqttVersion::MQTTv5, CountingRng(20000));
            c.add_client_id("esp32s3_ota_tb");
            c.add_username(config::THINGSBOARD_ACCESS_TOKEN); // ThingsBoard: token sbg username, tanpa password
            // TB_MQTT_BUF: lihat catatan lengkap di definisi konstantanya.
            c.max_packet_size = TB_MQTT_BUF as u32; // field ini u32, TB_MQTT_BUF usize (ukuran array)
            c
        };
        let mut recv_buffer = [0u8; TB_MQTT_BUF];
        let mut write_buffer = [0u8; TB_MQTT_BUF];
        let mut mqtt_client = MqttClient::<_, 5, _>::new(
            Io06(&mut socket),
            &mut write_buffer,
            TB_MQTT_BUF,
            &mut recv_buffer,
            TB_MQTT_BUF,
            mqtt_config,
        );

        match with_timeout(Duration::from_secs(10), mqtt_client.connect_to_broker()).await {
            Ok(Ok(())) => info!("[ThingsBoard] MQTT tersambung!"),
            Ok(Err(e)) => {
                warn!("[ThingsBoard] Gagal MQTT connect: {:?}", e);
                Timer::after(Duration::from_secs(5)).await;
                continue;
            }
            Err(_) => {
                warn!("[ThingsBoard] MQTT connect timeout (10 detik), coba lagi...");
                Timer::after(Duration::from_secs(5)).await;
                continue;
            }
        }

        // Subscribe SEBELUM minta (supaya tidak ketinggalan balasan), lalu
        // minta nilai attribute yang sedang tersimpan sekarang (kalau paket
        // sudah di-assign SEBELUM ESP32 ini menyala, push .../attributes
        // saja tidak akan memicu apa-apa -- makanya perlu REQUEST eksplisit).
        if mqtt_client
            .subscribe_to_topic("v1/devices/me/attributes/response/+")
            .await
            .is_err()
            || mqtt_client
                .subscribe_to_topic("v1/devices/me/attributes")
                .await
                .is_err()
        {
            warn!("[ThingsBoard] Gagal subscribe topik attribute, sambung ulang...");
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        if mqtt_client
            .send_message(
                "v1/devices/me/attributes/request/1",
                FW_ATTR_REQUEST.as_bytes(),
                QualityOfService::QoS0,
                false,
            )
            .await
            .is_err()
        {
            warn!("[ThingsBoard] Gagal minta attribute firmware, sambung ulang...");
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        info!(
            "[ThingsBoard] Subscribe OK, minta attribute firmware. Firmware saat ini: {} {}",
            FW_TITLE, FW_VERSION
        );

        let mut next_dht_ms: u64 = 0;

        'conn: loop {
            let now_ms = EmbInstant::now().as_millis();
            if now_ms >= next_dht_ms {
                match read_dht22_with_retry(&mut dht_pin, &mut dht_delay) {
                    Ok((temp, hum)) => {
                        let mut payload: FixedBuf<96> = FixedBuf::new();
                        let ok = write!(
                            payload,
                            "{{ \"temperature\": {:.1}, \"humidity\": {:.1} }}",
                            temp, hum
                        )
                        .is_ok();
                        if ok {
                            match mqtt_client
                                .send_message(
                                    config::THINGSBOARD_TOPIC,
                                    payload.as_bytes(),
                                    QualityOfService::QoS0,
                                    false,
                                )
                                .await
                            {
                                Ok(_) => info!("[ThingsBoard] Publish OK: {}", payload.as_str()),
                                Err(e) => {
                                    warn!("[ThingsBoard] Publish gagal: {:?}, sambung ulang...", e);
                                    break 'conn;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("[ThingsBoard] Gagal baca DHT22, dilewati sampel ini: {}", e);
                    }
                }
                next_dht_ms = EmbInstant::now().as_millis() + 5000;
            }

            let now_ms = EmbInstant::now().as_millis();
            let wait_ms = next_dht_ms.saturating_sub(now_ms).max(1);

            let (topic_owned, payload_owned, got_message): (FixedBuf<80>, alloc::vec::Vec<u8>, bool) = match with_timeout(
                Duration::from_millis(wait_ms),
                mqtt_client.receive_message(),
            )
            .await
            {
                Ok(Ok((topic, payload))) => {
                    // Salin dulu (topic ke buffer kecil, payload cuma dipakai
                    // untuk cek attribute di sini -- potongan firmware
                    // ditangani di loop unduh terpisah, bukan di sini).
                    let mut tbuf: FixedBuf<80> = FixedBuf::new();
                    let _ = write!(tbuf, "{}", topic);
                    let mut pbuf: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
                    pbuf.extend_from_slice(payload);
                    (tbuf, pbuf, true)
                }
                Ok(Err(e)) => {
                    warn!("[ThingsBoard] Gagal menerima pesan: {:?}, sambung ulang...", e);
                    break 'conn;
                }
                Err(_) => (FixedBuf::new(), alloc::vec::Vec::new(), false),
            };

            if !got_message {
                continue 'conn;
            }
            let topic = topic_owned.as_str();
            let is_attr_msg =
                topic == "v1/devices/me/attributes" || topic.starts_with("v1/devices/me/attributes/response/");
            if !is_attr_msg {
                continue 'conn;
            }

            let Ok(body) = core::str::from_utf8(&payload_owned) else {
                continue 'conn;
            };
            let Some(fw) = parse_fw_attrs(body) else {
                continue 'conn; // pesan attribute yang tidak berkaitan dengan firmware
            };
            if fw.title == FW_TITLE && fw.version == FW_VERSION {
                info!("[ThingsBoard] Firmware yang di-assign sama dengan yang sedang jalan, tidak ada yang perlu diunduh.");
                continue 'conn;
            }
            if fw.algorithm != "SHA256" {
                warn!(
                    "[ThingsBoard] Algoritma checksum '{}' tidak didukung (cuma SHA256), OTA dilewati.",
                    fw.algorithm
                );
                continue 'conn;
            }
            if ota_updater.is_none() {
                warn!("[ThingsBoard] OTA tidak tersedia (tabel partisi tidak punya slot OTA).");
                continue 'conn;
            }

            info!(
                "[ThingsBoard] Firmware BARU terdeteksi: {} {} ({} byte, checksum {}). Mulai unduh...",
                fw.title, fw.version, fw.size, fw.checksum
            );

            // Salin title/version/checksum SEBELUM subscribe/publish berikutnya
            // (supaya tidak menahan pinjaman ke buffer pesan yang lama).
            let mut fw_title_buf: FixedBuf<40> = FixedBuf::new();
            let mut fw_version_buf: FixedBuf<24> = FixedBuf::new();
            let mut fw_checksum_buf: FixedBuf<80> = FixedBuf::new();
            let _ = write!(fw_title_buf, "{}", fw.title);
            let _ = write!(fw_version_buf, "{}", fw.version);
            let _ = write!(fw_checksum_buf, "{}", fw.checksum);

            let mut tl: FixedBuf<160> = FixedBuf::new();
            let _ = build_fw_telemetry(&mut tl, fw_title_buf.as_str(), fw_version_buf.as_str(), "DOWNLOADING");
            publish_or_break!(mqtt_client, "v1/devices/me/telemetry", tl.as_bytes(), 'conn);

            if mqtt_client
                .subscribe_to_topic("v2/fw/response/+/chunk/+")
                .await
                .is_err()
            {
                warn!("[ThingsBoard] Gagal subscribe potongan firmware, sambung ulang...");
                break 'conn;
            }

            let mut hasher = Sha256::new();
            let mut offset: u32 = 0;
            let mut ok = true;
            let mut fail_reason = "";

            'download: for chunk_index in 0..OTA_MAX_CHUNKS {
                let mut req_topic: FixedBuf<64> = FixedBuf::new();
                if build_chunk_request_topic(&mut req_topic, 1, chunk_index).is_err() {
                    ok = false;
                    fail_reason = "internal_error";
                    break 'download;
                }
                let mut size_req: FixedBuf<16> = FixedBuf::new();
                let _ = write!(size_req, "{}", OTA_CHUNK_MAX);
                if mqtt_client
                    .send_message(req_topic.as_str(), size_req.as_bytes(), QualityOfService::QoS0, false)
                    .await
                    .is_err()
                {
                    ok = false;
                    fail_reason = "publish_failed";
                    break 'download;
                }

                // Tunggu balasan potongan. Pesan LAIN (mis. push attribute
                // lain yang kebetulan lewat) diabaikan dan kita tetap
                // menunggu -- HANYA topik "v2/fw/response/" yang dianggap
                // jawaban potongan ini.
                let chunk_len = loop {
                    match with_timeout(Duration::from_millis(OTA_STALL_MS), mqtt_client.receive_message()).await {
                        Ok(Ok((t, p))) => {
                            if t.starts_with("v2/fw/response/") {
                                // PENTING: pakai PERSIS panjang `p` yang diterima, JANGAN
                                // dipotong ke OTA_CHUNK_MAX. ThingsBoard terbukti mengabaikan
                                // ukuran yang kita minta (pernah kirim ~2075 byte utk
                                // permintaan 1024) -- memotong berarti membuang sisa data
                                // dan bikin posisi tulis flash meleset dari isi file asli.
                                let n = p.len();
                                if ota_write_at(ota_updater.as_mut().unwrap(), offset, &p[..n]).is_ok() {
                                    hasher.update(&p[..n]);
                                    break n;
                                } else {
                                    break usize::MAX; // tandai gagal tulis
                                }
                            }
                            // topik lain, abaikan dan tunggu lagi
                        }
                        Ok(Err(e)) => {
                            warn!("[ThingsBoard] Gagal menerima potongan: {:?}", e);
                            break usize::MAX;
                        }
                        Err(_) => {
                            warn!("[ThingsBoard] Tidak ada balasan potongan #{} dalam {} ms.", chunk_index, OTA_STALL_MS);
                            break usize::MAX;
                        }
                    }
                };

                if chunk_len == usize::MAX {
                    ok = false;
                    fail_reason = "flash_error_or_timeout";
                    break 'download;
                }
                if chunk_len == 0 {
                    info!("[ThingsBoard] Unduhan selesai: {} byte diterima.", offset);
                    break 'download; // potongan kosong = tanda selesai
                }
                offset += chunk_len as u32;
                if offset > fw.size.max(offset) + (OTA_CHUNK_MAX as u32) {
                    // pengaman kasar: jauh melebihi fw_size yang dijanjikan
                    ok = false;
                    fail_reason = "size_mismatch";
                    break 'download;
                }
                info!("[ThingsBoard] Unduh: {} byte (potongan #{})", offset, chunk_index);
            }

            if ok {
                let digest: [u8; 32] = hasher.finalize().into();
                let mut digest_hex: FixedBuf<64> = FixedBuf::new();
                let _ = hex_encode_32(&mut digest_hex, &digest);
                if digest_hex.as_str().eq_ignore_ascii_case(fw_checksum_buf.as_str()) {
                    info!("[ThingsBoard] SHA-256 cocok. Mengaktifkan partisi baru...");
                    let activated = ota_updater.as_mut().is_some_and(|u| {
                        u.activate_next_partition().is_ok()
                            && u.set_current_ota_state(OtaImageState::New).is_ok()
                    });
                    if activated {
                        let mut tl: FixedBuf<160> = FixedBuf::new();
                        let _ = build_fw_telemetry(&mut tl, fw_title_buf.as_str(), fw_version_buf.as_str(), "UPDATED");
                        let _ = mqtt_client
                            .send_message("v1/devices/me/telemetry", tl.as_bytes(), QualityOfService::QoS0, false)
                            .await;
                        info!("[ThingsBoard] OTA selesai. Reboot dalam 1,5 detik...");
                        Timer::after(Duration::from_millis(1500)).await;
                        esp_hal::system::software_reset();
                    } else {
                        ok = false;
                        fail_reason = "activate_failed";
                    }
                } else {
                    warn!(
                        "[ThingsBoard] SHA-256 TIDAK COCOK. Dihitung: {} | Diharapkan: {}. Firmware LAMA tetap dipakai.",
                        digest_hex.as_str(),
                        fw_checksum_buf.as_str()
                    );
                    ok = false;
                    fail_reason = "checksum_mismatch";
                }
            }

            if !ok {
                warn!("[ThingsBoard] OTA GAGAL: {}", fail_reason);
                let mut tl: FixedBuf<160> = FixedBuf::new();
                let _ = build_fw_telemetry(&mut tl, fw_title_buf.as_str(), fw_version_buf.as_str(), "FAILED");
                let _ = mqtt_client
                    .send_message("v1/devices/me/telemetry", tl.as_bytes(), QualityOfService::QoS0, false)
                    .await;
            }
        }

        Timer::after(Duration::from_secs(5)).await;
    }
}

// ===========================================================================
// DHT22 — DISALIN PERSIS dari hive_rec/hive_ml (sudah terbukti jalan di ESP32
// ini). Jangan diubah.
// ===========================================================================

/// Baca DHT22 dengan retry diam-diam sampai 5x sebelum benar-benar dianggap gagal.
fn read_dht22_with_retry(pin: &mut Flex, delay: &mut Delay) -> Result<(f32, f32), &'static str> {
    let mut last_err = "Belum ada percobaan";

    for attempt in 0..5 {
        let result = critical_section::with(|_cs| read_dht22(pin, delay));

        match result {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = e;
                if attempt < 4 {
                    delay.delay_millis(300);
                }
            }
        }
    }

    Err(last_err)
}

/// Baca sensor DHT22. Return (temperature_celsius, humidity_percent) atau pesan error.
fn read_dht22(pin: &mut Flex, delay: &mut Delay) -> Result<(f32, f32), &'static str> {
    pin.set_output_enable(true);
    pin.set_low();
    delay.delay_millis(2);

    pin.set_output_enable(false);
    delay.delay_micros(30);

    wait_for_level(pin, false, 200)?;
    wait_for_level(pin, true, 200)?;
    wait_for_level(pin, false, 200)?;

    let mut data = [0u8; 5];
    for byte in data.iter_mut() {
        for _ in 0..8 {
            wait_for_level(pin, true, 150)?;
            let high_us = measure_high_duration(pin, 150)?;
            *byte <<= 1;
            if high_us > 40 {
                *byte |= 1;
            }
        }
    }

    pin.set_output_enable(true);
    pin.set_high();

    let checksum = data[0]
        .wrapping_add(data[1])
        .wrapping_add(data[2])
        .wrapping_add(data[3]);
    if checksum != data[4] {
        return Err("Checksum tidak cocok");
    }

    let humidity = (((data[0] as u16) << 8) | data[1] as u16) as f32 / 10.0;
    let temp_raw = (((data[2] as u16) & 0x7F) << 8) | data[3] as u16;
    let mut temperature = temp_raw as f32 / 10.0;
    if data[2] & 0x80 != 0 {
        temperature = -temperature;
    }

    Ok((temperature, humidity))
}

fn wait_for_level(pin: &Flex, want_high: bool, timeout_us: u32) -> Result<(), &'static str> {
    let start = Instant::now();
    loop {
        let is_high = pin.is_high();
        if is_high == want_high {
            return Ok(());
        }
        if start.elapsed().as_micros() as u32 > timeout_us {
            return Err("Timeout menunggu perubahan level pin");
        }
    }
}

fn measure_high_duration(pin: &Flex, timeout_us: u32) -> Result<u32, &'static str> {
    let start = Instant::now();
    loop {
        if pin.is_low() {
            return Ok(start.elapsed().as_micros() as u32);
        }
        if start.elapsed().as_micros() as u32 > timeout_us {
            return Err("Timeout mengukur durasi high");
        }
    }
}