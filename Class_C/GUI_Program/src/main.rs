use eframe::egui;
use printpdf::*;
use rumqttc::{Client, Event, MqttOptions, Packet, QoS, Transport};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TestFsmState {
    Idle,
    Testing,
    Sampling,
    Verification,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AppPage {
    Testing,
    FirmwareOta,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OtaStatus {
    Idle,
    Uploading,
    Success,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferenceResult {
    #[serde(alias = "label")]
    pub class: String,
    pub confidence: f32,
    pub inference_time_ms: u32,
    #[serde(default)]
    pub raw_data: Vec<f32>,
}

#[derive(Clone, Debug)]
pub struct TestRecord {
    pub timestamp: String,
    pub label: String,
    pub confidence: f32,
    pub inference_time_ms: u32,
    pub validation_status: String,
    pub ground_truth: String,
}

pub enum MqttCommand {
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
    },
}

pub enum AppEvent {
    Connected,
    Disconnected(String),
    ResultReceived(InferenceResult),
    OtaAckReceived,
    OtaProgress(f32),
    OtaFinished(Result<(), String>),
    EdgeImpulseUploaded(Result<String, String>),
}

pub struct AppState {
    current_page: AppPage,
    mqtt_connected: bool,
    tx_cmd: Sender<MqttCommand>,
    tx_event: Sender<AppEvent>,
    rx_event: Receiver<AppEvent>,
    fsm_state: TestFsmState,
    sampling_start_time: Option<Instant>,
    notification_msg: Option<String>,
    notification_is_error: bool,
    current_result: Option<InferenceResult>,
    misclassified_active: bool,
    selected_ground_truth: String,
    available_classes: Vec<String>,
    test_records: Vec<TestRecord>,
    selected_ota_path: Option<PathBuf>,
    ota_file_data: Option<Vec<u8>>,
    ota_status: OtaStatus,
    ota_progress: f32,
    ota_error_message: Option<String>,
    shared_ota_ack_sender: Arc<Mutex<Option<Sender<()>>>>,
}

impl AppState {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (tx_cmd, rx_cmd) = channel::<MqttCommand>();
        let (tx_event, rx_event) = channel::<AppEvent>();
        let shared_ota_ack_sender: Arc<Mutex<Option<Sender<()>>>> = Arc::new(Mutex::new(None));

        let client_id = format!(
            "enose_client_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        let mut mqttoptions = MqttOptions::new(
            client_id,
            "84aec46d41534b009368ec20fc74362c.s1.eu.hivemq.cloud",
            8883,
        );
        mqttoptions.set_credentials("YOUR_MQTT_USERNAME", "YOUR_MQTT_PASSWORD");
        mqttoptions.set_keep_alive(Duration::from_secs(15));
        mqttoptions.set_transport(Transport::tls_with_default_config());

        let (client, mut connection) = Client::new(mqttoptions, 64);

        let tx_event_poller = tx_event.clone();
        let ack_forwarder = Arc::clone(&shared_ota_ack_sender);

        thread::spawn(move || {
            let _ = client.subscribe("esp32/data/result", QoS::AtLeastOnce);
            let _ = client.subscribe("esp32/ota/ack", QoS::AtLeastOnce);

            let poller_client = client.clone();
            thread::spawn(move || {
                while let Ok(cmd) = rx_cmd.recv() {
                    match cmd {
                        MqttCommand::Publish { topic, payload, qos } => {
                            let _ = poller_client.publish(topic, qos, false, payload);
                        }
                    }
                }
            });

            for notification in connection.iter() {
                match notification {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        let _ = tx_event_poller.send(AppEvent::Connected);
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if publish.topic == "esp32/data/result" {
                            if let Some(res) = Self::parse_result_payload(&publish.payload) {
                                let _ = tx_event_poller.send(AppEvent::ResultReceived(res));
                            }
                        } else if publish.topic == "esp32/ota/ack" {
                            if let Ok(guard) = ack_forwarder.lock() {
                                if let Some(ref sender) = *guard {
                                    let _ = sender.send(());
                                }
                            }
                            let _ = tx_event_poller.send(AppEvent::OtaAckReceived);
                        }
                    }
                    Err(e) => {
                        let _ = tx_event_poller.send(AppEvent::Disconnected(e.to_string()));
                    }
                    _ => {}
                }
            }
        });

        Self {
            current_page: AppPage::Testing,
            mqtt_connected: false,
            tx_cmd,
            tx_event,
            rx_event,
            fsm_state: TestFsmState::Idle,
            sampling_start_time: None,
            notification_msg: None,
            notification_is_error: false,
            current_result: None,
            misclassified_active: false,
            selected_ground_truth: "Sample_A".to_string(),
            available_classes: vec![
                "Sample_A".to_string(),
                "Sample_B".to_string(),
                "Sample_C".to_string(),
                "Sample_D".to_string(),
                "Sample_E".to_string(),
                "Sample_F".to_string(),
                "Sample_G".to_string(),
                "Sample_H".to_string(),
                "Sample_I".to_string(),
                "Sample_J".to_string(),
                "Sample_K".to_string(),
                "Sample_L".to_string(),
                "Sample_M".to_string(),
                "Sample_N".to_string(),
                "Sample_O".to_string(),
                "Sample_P".to_string(),
                "Sample_Q".to_string(),
                "Sample_R".to_string(),
            ],
            test_records: Vec::new(),
            selected_ota_path: None,
            ota_file_data: None,
            ota_status: OtaStatus::Idle,
            ota_progress: 0.0,
            ota_error_message: None,
            shared_ota_ack_sender,
        }
    }

    fn parse_result_payload(payload: &[u8]) -> Option<InferenceResult> {
        if let Ok(mut res) = serde_json::from_slice::<InferenceResult>(payload) {
            if res.confidence <= 1.0 {
                res.confidence *= 100.0;
            }
            res.class = res
                .class
                .trim_matches(|c| c == '{' || c == '}' || c == '"' || c == ' ')
                .to_string();
            return Some(res);
        }
        None
    }

    fn current_timestamp_str() -> String {
        let total_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + (7 * 3600);
        let hours = (total_secs / 3600) % 24;
        let minutes = (total_secs / 60) % 60;
        let seconds = total_secs % 60;
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }

    fn generate_pdf_report(&self) -> Result<(), String> {
        use printpdf::path::{PaintMode, WindingOrder};

        if self.test_records.is_empty() {
            return Err("No test records available to export".to_string());
        }

        const HUPOMONE_LOGO: &[u8] = include_bytes!("../assets/hupomone.png");

        let (doc, page1, layer1) =
            PdfDocument::new("E-Nose Validation Report", Mm(210.0), Mm(297.0), "Layer 1");
        let layer = doc.get_page(page1).get_layer(layer1);

        let font_bold = doc
            .add_builtin_font(BuiltinFont::HelveticaBold)
            .map_err(|e| e.to_string())?;
        let font_reg = doc
            .add_builtin_font(BuiltinFont::Helvetica)
            .map_err(|e| e.to_string())?;

        let draw_rect = |layer_ref: &PdfLayerReference,
                         x: f32,
                         y: f32,
                         w: f32,
                         h: f32,
                         fill: Option<Color>,
                         stroke: Option<Color>,
                         stroke_w: f32| {
            if let Some(ref f) = fill {
                layer_ref.set_fill_color(f.clone());
            }
            if let Some(ref s) = stroke {
                layer_ref.set_outline_color(s.clone());
                layer_ref.set_outline_thickness(stroke_w);
            }
            let points = vec![
                (Point::new(Mm(x), Mm(y)), false),
                (Point::new(Mm(x + w), Mm(y)), false),
                (Point::new(Mm(x + w), Mm(y + h)), false),
                (Point::new(Mm(x), Mm(y + h)), false),
            ];
            let mode = match (fill.is_some(), stroke.is_some()) {
                (true, true) => PaintMode::FillStroke,
                (true, false) => PaintMode::Fill,
                (false, true) => PaintMode::Stroke,
                (false, false) => PaintMode::Clip,
            };
            layer_ref.add_polygon(Polygon {
                rings: vec![points],
                mode,
                winding_order: WindingOrder::NonZero,
            });
        };

        let draw_line = |layer_ref: &PdfLayerReference,
                         x1: f32,
                         y1: f32,
                         x2: f32,
                         y2: f32,
                         stroke: Color,
                         stroke_w: f32| {
            layer_ref.set_outline_color(stroke);
            layer_ref.set_outline_thickness(stroke_w);
            let points = vec![
                (Point::new(Mm(x1), Mm(y1)), false),
                (Point::new(Mm(x2), Mm(y2)), false),
            ];
            layer_ref.add_line(Line {
                points,
                is_closed: false,
            });
        };

        if let Ok(img) = ::image::load_from_memory(HUPOMONE_LOGO) {
            let rgba = img.to_rgba8();
            let (w, h) = rgba.dimensions();
            let mut blended_rgb = Vec::with_capacity((w * h * 3) as usize);
            let alpha_factor = 0.08_f32;
            for pixel in rgba.pixels() {
                let a = (pixel[3] as f32 / 255.0) * alpha_factor;
                let r = ((pixel[0] as f32 * a) + (255.0 * (1.0 - a))) as u8;
                let g = ((pixel[1] as f32 * a) + (255.0 * (1.0 - a))) as u8;
                let b = ((pixel[2] as f32 * a) + (255.0 * (1.0 - a))) as u8;
                blended_rgb.push(r);
                blended_rgb.push(g);
                blended_rgb.push(b);
            }
            let image_xobject = ImageXObject {
                width: Px(w as usize),
                height: Px(h as usize),
                color_space: ColorSpace::Rgb,
                bits_per_component: ColorBits::Bit8,
                interpolate: true,
                image_data: blended_rgb,
                image_filter: None,
                clipping_bbox: None,
                smask: None,
            };
            let pdf_image = Image::from(image_xobject);
            let target_w_mm = 52.0_f32;
            let target_h_mm = 52.0_f32 * (h as f32 / w as f32);
            let center_x_mm = 105.0_f32 - (target_w_mm / 2.0);
            let center_y_mm = 148.5_f32 - (target_h_mm / 2.0);
            let scale_x = target_w_mm / (w as f32 * 0.084667);
            let scale_y = target_h_mm / (h as f32 * 0.084667);
            pdf_image.add_to_layer(
                layer.clone(),
                ImageTransform {
                    translate_x: Some(Mm(center_x_mm)),
                    translate_y: Some(Mm(center_y_mm)),
                    scale_x: Some(scale_x),
                    scale_y: Some(scale_y),
                    ..Default::default()
                },
            );

            layer.set_fill_color(Color::Rgb(Rgb::new(0.92, 0.92, 0.93, None)));
            layer.use_text(
                "HUPOMONE",
                16.0,
                Mm(88.4),
                Mm(center_y_mm - 7.5),
                &font_bold,
            );
        }

        layer.set_fill_color(Color::Rgb(Rgb::new(0.08, 0.18, 0.32, None)));
        layer.use_text(
            "E-NOSE AI INFERENCE AND VALIDATION REPORT",
            14.0,
            Mm(37.0),
            Mm(281.0),
            &font_bold,
        );

        draw_line(
            &layer,
            15.0,
            277.0,
            195.0,
            277.0,
            Color::Rgb(Rgb::new(0.10, 0.25, 0.45, None)),
            1.2,
        );
        draw_line(
            &layer,
            15.0,
            275.8,
            195.0,
            275.8,
            Color::Rgb(Rgb::new(0.72, 0.77, 0.85, None)),
            0.4,
        );

        draw_rect(
            &layer,
            15.0,
            248.0,
            180.0,
            24.0,
            Some(Color::Rgb(Rgb::new(0.96, 0.97, 0.98, None))),
            Some(Color::Rgb(Rgb::new(0.80, 0.84, 0.88, None))),
            0.5,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.12, 0.15, 0.20, None)));
        layer.use_text("Device ID", 7.5, Mm(18.0), Mm(266.0), &font_bold);
        layer.use_text(": ESP32-S3", 7.5, Mm(40.0), Mm(266.0), &font_reg);

        layer.use_text("Target Model", 7.5, Mm(18.0), Mm(259.5), &font_bold);
        layer.use_text(": TinyML", 7.5, Mm(40.0), Mm(259.5), &font_reg);

        layer.use_text("Operator Name", 7.5, Mm(18.0), Mm(253.0), &font_bold);
        layer.use_text(
            ": Ahmad Fauzi Abdul Razzaq, Ilham Zain Muttaqin",
            6.2,
            Mm(40.0),
            Mm(253.0),
            &font_reg,
        );

        let date_str = format!("2026-09-07 {}", AppState::current_timestamp_str());
        layer.use_text("Tanggal Pengujian", 7.0, Mm(102.0), Mm(266.0), &font_bold);
        layer.use_text(format!(": {}", date_str), 7.0, Mm(128.0), Mm(266.0), &font_reg);

        layer.use_text("Host HiveMQ", 7.0, Mm(102.0), Mm(259.5), &font_bold);
        layer.use_text(
            ": 84aec46d41534b009368ec20fc74362c.s1.eu.hivemq.cloud",
            5.8,
            Mm(128.0),
            Mm(259.5),
            &font_reg,
        );

        layer.use_text("Port & Security", 7.0, Mm(102.0), Mm(253.0), &font_bold);
        layer.use_text(": 8883 (TLS Encrypted)", 7.0, Mm(128.0), Mm(253.0), &font_reg);

        let total_samples = self.test_records.len();
        let accurate_count = self
            .test_records
            .iter()
            .filter(|r| r.validation_status == "Verified")
            .count();
        let misclassified_count = total_samples - accurate_count;
        let accuracy_rate = if total_samples > 0 {
            (accurate_count as f32 / total_samples as f32) * 100.0
        } else {
            0.0
        };

        draw_rect(
            &layer,
            15.0,
            223.0,
            180.0,
            21.0,
            Some(Color::Rgb(Rgb::new(0.98, 0.99, 1.0, None))),
            Some(Color::Rgb(Rgb::new(0.75, 0.80, 0.88, None))),
            0.5,
        );

        draw_rect(
            &layer,
            15.0,
            238.0,
            180.0,
            6.0,
            Some(Color::Rgb(Rgb::new(0.92, 0.94, 0.97, None))),
            None,
            0.0,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.18, 0.24, 0.35, None)));
        layer.use_text(
            "PERFORMANCE SUMMARY METRICS",
            7.0,
            Mm(18.0),
            Mm(239.8),
            &font_bold,
        );

        draw_line(
            &layer,
            15.0,
            238.0,
            195.0,
            238.0,
            Color::Rgb(Rgb::new(0.82, 0.86, 0.92, None)),
            0.4,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.40, 0.45, 0.52, None)));
        layer.use_text("Total Samples", 6.8, Mm(20.0), Mm(233.0), &font_reg);
        layer.set_fill_color(Color::Rgb(Rgb::new(0.10, 0.15, 0.22, None)));
        layer.use_text(
            format!("{}", total_samples),
            10.0,
            Mm(20.0),
            Mm(226.5),
            &font_bold,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.40, 0.45, 0.52, None)));
        layer.use_text("Accurate Count", 6.8, Mm(65.0), Mm(233.0), &font_reg);
        layer.set_fill_color(Color::Rgb(Rgb::new(0.12, 0.60, 0.28, None)));
        layer.use_text(
            format!("{}", accurate_count),
            10.0,
            Mm(65.0),
            Mm(226.5),
            &font_bold,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.40, 0.45, 0.52, None)));
        layer.use_text("Misclassified Count", 6.8, Mm(110.0), Mm(233.0), &font_reg);
        layer.set_fill_color(Color::Rgb(Rgb::new(0.78, 0.16, 0.16, None)));
        layer.use_text(
            format!("{}", misclassified_count),
            10.0,
            Mm(110.0),
            Mm(226.5),
            &font_bold,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(0.40, 0.45, 0.52, None)));
        layer.use_text("Model Accuracy", 6.8, Mm(155.0), Mm(233.0), &font_reg);
        layer.set_fill_color(Color::Rgb(Rgb::new(0.10, 0.25, 0.55, None)));
        layer.use_text(
            format!("{:.2}%", accuracy_rate),
            10.0,
            Mm(155.0),
            Mm(226.5),
            &font_bold,
        );

        let table_top_y: f32 = 217.0;
        let header_h: f32 = 6.5;
        let cols: [(f32, f32, &str); 6] = [
            (15.0, 12.0, "No"),
            (27.0, 26.0, "Timestamp"),
            (53.0, 42.0, "Predicted Class"),
            (95.0, 25.0, "Confidence"),
            (120.0, 35.0, "Validation Status"),
            (155.0, 40.0, "Ground Truth"),
        ];

        draw_rect(
            &layer,
            15.0,
            table_top_y - header_h,
            180.0,
            header_h,
            Some(Color::Rgb(Rgb::new(0.10, 0.21, 0.36, None))),
            Some(Color::Rgb(Rgb::new(0.08, 0.16, 0.28, None))),
            0.5,
        );

        layer.set_fill_color(Color::Rgb(Rgb::new(1.0, 1.0, 1.0, None)));
        for &(cx, _, title) in &cols {
            layer.use_text(title, 7.5, Mm(cx + 2.0), Mm(table_top_y - 4.6), &font_bold);
        }

        let mut current_row_y: f32 = table_top_y - header_h;
        let row_h: f32 = 5.6;

        for (i, item) in self.test_records.iter().enumerate() {
            if current_row_y - row_h < 20.0 {
                break;
            }

            let bg_color = None;

            draw_rect(
                &layer,
                15.0,
                current_row_y - row_h,
                180.0,
                row_h,
                bg_color,
                Some(Color::Rgb(Rgb::new(0.85, 0.88, 0.92, None))),
                0.3,
            );

            for &(cx, _, _) in &cols[1..] {
                draw_line(
                    &layer,
                    cx,
                    current_row_y,
                    cx,
                    current_row_y - row_h,
                    Color::Rgb(Rgb::new(0.85, 0.88, 0.92, None)),
                    0.3,
                );
            }

            let text_y: f32 = current_row_y - 4.0;

            layer.set_fill_color(Color::Rgb(Rgb::new(0.15, 0.18, 0.22, None)));
            layer.use_text(
                format!("{}", i + 1),
                7.2,
                Mm(cols[0].0 + 2.0),
                Mm(text_y),
                &font_reg,
            );
            layer.use_text(&item.timestamp, 7.0, Mm(cols[1].0 + 2.0), Mm(text_y), &font_reg);
            layer.use_text(&item.label, 7.2, Mm(cols[2].0 + 2.0), Mm(text_y), &font_bold);
            layer.use_text(
                format!("{:.2}%", item.confidence),
                7.2,
                Mm(cols[3].0 + 2.0),
                Mm(text_y),
                &font_reg,
            );

            if item.validation_status == "Verified" {
                layer.set_fill_color(Color::Rgb(Rgb::new(0.12, 0.60, 0.28, None)));
                layer.use_text(
                    "Verified",
                    7.2,
                    Mm(cols[4].0 + 2.0),
                    Mm(text_y),
                    &font_bold,
                );
            } else {
                layer.set_fill_color(Color::Rgb(Rgb::new(0.78, 0.16, 0.16, None)));
                layer.use_text(
                    "Misclassified",
                    7.2,
                    Mm(cols[4].0 + 2.0),
                    Mm(text_y),
                    &font_bold,
                );
            }

            layer.set_fill_color(Color::Rgb(Rgb::new(0.15, 0.18, 0.22, None)));
            layer.use_text(
                &item.ground_truth,
                7.2,
                Mm(cols[5].0 + 2.0),
                Mm(text_y),
                &font_reg,
            );

            current_row_y -= row_h;
        }

        draw_line(
            &layer,
            15.0,
            13.0,
            195.0,
            13.0,
            Color::Rgb(Rgb::new(0.82, 0.85, 0.90, None)),
            0.5,
        );
        layer.set_fill_color(Color::Rgb(Rgb::new(0.50, 0.55, 0.62, None)));
        layer.use_text(
            "Created by Instrumentation Engineering Students at the Vocational Faculty of the Institut Teknologi Sepuluh Nopember",
            7.5,
            Mm(31.0),
            Mm(8.5),
            &font_reg,
        );

        let file_path = "enose_test_report.pdf";
        let file = File::create(file_path).map_err(|e| e.to_string())?;
        doc.save(&mut BufWriter::new(file))
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

pub struct EdgeAiApp {
    state: AppState,
    logo_texture: Option<egui::TextureHandle>,
}

impl EdgeAiApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let logo_texture =
            if let Ok(img) = ::image::load_from_memory(include_bytes!("../assets/logo.png")) {
                let size = [img.width() as usize, img.height() as usize];
                let rgba = img.to_rgba8();
                let color_image = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw());
                Some(cc.egui_ctx.load_texture("gui_logo", color_image, Default::default()))
            } else {
                None
            };

        Self {
            state: AppState::new(cc),
            logo_texture,
        }
    }

    fn industrial_card_frame() -> egui::Frame {
        egui::Frame::none()
            .fill(egui::Color32::from_rgb(24, 27, 34))
            .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(44, 50, 62)))
            .inner_margin(egui::Margin::same(12.0))
            .rounding(egui::Rounding::same(6.0))
    }

    fn confidence_color(conf: f32) -> egui::Color32 {
        let factor = ((conf - 50.0).max(0.0) / 50.0).clamp(0.0, 1.0);
        let r = 240.0 - (195.0 * factor);
        let g = 190.0 + (35.0 * factor);
        let b = 40.0 + (70.0 * factor);
        egui::Color32::from_rgb(r as u8, g as u8, b as u8)
    }

    fn start_ota_upload_process(&mut self) {
        let file_bytes = match &self.state.ota_file_data {
            Some(b) => b.clone(),
            None => return,
        };

        self.state.ota_status = OtaStatus::Uploading;
        self.state.ota_progress = 0.0;
        self.state.ota_error_message = None;

        let (tx_worker_ack, rx_worker_ack) = channel::<()>();
        if let Ok(mut guard) = self.state.shared_ota_ack_sender.lock() {
            *guard = Some(tx_worker_ack);
        }

        let main_tx_cmd = self.state.tx_cmd.clone();
        let total_bytes = file_bytes.len();
        let chunk_size = 1024;
        let total_chunks = (total_bytes + chunk_size - 1) / chunk_size;

        let bridge_tx = self.state.tx_event.clone();
        let ack_cleaner = Arc::clone(&self.state.shared_ota_ack_sender);

        thread::spawn(move || {
            let mut current_chunk = 0;
            for chunk in file_bytes.chunks(chunk_size) {
                let _ = main_tx_cmd.send(MqttCommand::Publish {
                    topic: "esp32/ota/payload".to_string(),
                    payload: chunk.to_vec(),
                    qos: QoS::AtMostOnce,
                });

                match rx_worker_ack.recv_timeout(Duration::from_secs(15)) {
                    Ok(()) => {
                        current_chunk += 1;
                        let progress = current_chunk as f32 / total_chunks as f32;
                        let _ = bridge_tx.send(AppEvent::OtaProgress(progress));
                    }
                    Err(_) => {
                        if let Ok(mut guard) = ack_cleaner.lock() {
                            *guard = None;
                        }
                        let _ = bridge_tx.send(AppEvent::OtaFinished(Err(
                            "Target MCU ACK timeout expired (15s)".to_string(),
                        )));
                        return;
                    }
                }
            }
            if let Ok(mut guard) = ack_cleaner.lock() {
                *guard = None;
            }
            let _ = bridge_tx.send(AppEvent::OtaFinished(Ok(())));
        });
    }

    fn render_testing_view(&mut self, ui: &mut egui::Ui) {
        let available_width = ui.available_width();
        let spacing = ui.spacing().item_spacing.x;
        let col_width = (available_width - spacing) * 0.5;

        ui.horizontal_top(|ui| {
            ui.allocate_ui_with_layout(
                egui::vec2(col_width, ui.available_height()),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    Self::industrial_card_frame().show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new("System State").strong());
                            ui.separator();
                            let (status_str, status_clr) = match self.state.fsm_state {
                                TestFsmState::Idle => ("Idle", egui::Color32::from_rgb(140, 150, 165)),
                                TestFsmState::Testing => {
                                    ("Testing", egui::Color32::from_rgb(52, 152, 219))
                                }
                                TestFsmState::Sampling => {
                                    ("Sampling", egui::Color32::from_rgb(241, 196, 15))
                                }
                                TestFsmState::Verification => {
                                    ("Verification", egui::Color32::from_rgb(46, 204, 113))
                                }
                            };
                            ui.colored_label(status_clr, egui::RichText::new(status_str).strong());
                        });

                        ui.add_space(10.0);

                        ui.horizontal(|ui| {
                            let can_start = self.state.fsm_state == TestFsmState::Idle;
                            if ui
                                .add_enabled(can_start, egui::Button::new("Start Testing"))
                                .clicked()
                            {
                                self.state.fsm_state = TestFsmState::Testing;
                                self.state.notification_msg = None;
                            }

                            let can_stop = self.state.fsm_state != TestFsmState::Idle;
                            if ui
                                .add_enabled(can_stop, egui::Button::new("Stop Testing"))
                                .clicked()
                            {
                                self.state.fsm_state = TestFsmState::Idle;
                                self.state.sampling_start_time = None;
                                self.state.current_result = None;
                                self.state.misclassified_active = false;
                            }

                            let can_sample = self.state.fsm_state == TestFsmState::Testing;
                            let sampling_txt = if self.state.fsm_state == TestFsmState::Sampling {
                                "Sampling Active"
                            } else {
                                "Sampling"
                            };
                            if ui
                                .add_enabled(can_sample, egui::Button::new(sampling_txt))
                                .clicked()
                            {
                                self.state.fsm_state = TestFsmState::Sampling;
                                self.state.sampling_start_time = Some(Instant::now());
                                self.state.notification_msg = None;
                                let _ = self.state.tx_cmd.send(MqttCommand::Publish {
                                    topic: "esp32/cmd/sampling".to_string(),
                                    payload: b"START_SAMPLING".to_vec(),
                                    qos: QoS::AtLeastOnce,
                                });
                            }

                            let can_export = self.state.fsm_state == TestFsmState::Idle
                                && !self.state.test_records.is_empty();
                            if ui
                                .add_enabled(can_export, egui::Button::new("Export PDF"))
                                .clicked()
                            {
                                match self.state.generate_pdf_report() {
                                    Ok(()) => {
                                        self.state.notification_msg = Some(
                                            "Document successfully exported to enose_test_report.pdf"
                                                .to_string(),
                                        );
                                        self.state.notification_is_error = false;
                                    }
                                    Err(e) => {
                                        self.state.notification_msg = Some(e);
                                        self.state.notification_is_error = true;
                                    }
                                }
                            }
                        });
                    });

                    ui.add_space(10.0);

                    Self::industrial_card_frame().show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.heading("Inference Result");
                        ui.add_space(6.0);

                        if let Some(ref res) = self.state.current_result {
                            ui.label(
                                egui::RichText::new(&res.class)
                                    .size(24.0)
                                    .strong()
                                    .color(egui::Color32::WHITE),
                            );

                            ui.add_space(4.0);
                            ui.horizontal(|ui| {
                                ui.label("Confidence Score:");
                                ui.strong(format!("{:.2}%", res.confidence));
                            });

                            let conf_fraction = (res.confidence / 100.0).clamp(0.0, 1.0);
                            let bar_color = Self::confidence_color(res.confidence);
                            let bar = egui::ProgressBar::new(conf_fraction)
                                .show_percentage()
                                .fill(bar_color)
                                .animate(false);
                            ui.add(bar);

                            ui.add_space(4.0);
                            ui.horizontal(|ui| {
                                ui.label("Compute Time:");
                                ui.strong(format!("{} ms", res.inference_time_ms));
                            });
                        } else {
                            ui.label(
                                egui::RichText::new("Awaiting acquisition payload...")
                                    .color(egui::Color32::from_rgb(120, 130, 145)),
                            );
                            let bar = egui::ProgressBar::new(0.0).fill(egui::Color32::from_rgb(60, 65, 75));
                            ui.add(bar);
                            ui.label("Compute Time: -- ms");
                        }
                    });

                    ui.add_space(10.0);

                    Self::industrial_card_frame().show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.heading("Verification");
                        ui.add_space(6.0);

                        let can_verify = self.state.fsm_state == TestFsmState::Verification;
                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(can_verify, egui::Button::new("Verified"))
                                .clicked()
                            {
                                if let Some(ref res) = self.state.current_result {
                                    self.state.test_records.push(TestRecord {
                                        timestamp: AppState::current_timestamp_str(),
                                        label: res.class.clone(),
                                        confidence: res.confidence,
                                        inference_time_ms: res.inference_time_ms,
                                        validation_status: "Verified".to_string(),
                                        ground_truth: res.class.clone(),
                                    });
                                    self.state.current_result = None;
                                    self.state.misclassified_active = false;
                                    self.state.fsm_state = TestFsmState::Testing;
                                }
                            }

                            if ui
                                .add_enabled(can_verify, egui::Button::new("Misclassified"))
                                .clicked()
                            {
                                self.state.misclassified_active = true;
                            }
                        });

                        if can_verify && self.state.misclassified_active {
                            ui.add_space(8.0);
                            ui.separator();
                            ui.label("Ground Truth Label");
                            egui::ComboBox::from_id_salt("ground_truth_selector")
                                .selected_text(&self.state.selected_ground_truth)
                                .width(ui.available_width() - 20.0)
                                .show_ui(ui, |ui| {
                                    for class_name in &self.state.available_classes {
                                        ui.selectable_value(
                                            &mut self.state.selected_ground_truth,
                                            class_name.clone(),
                                            class_name,
                                        );
                                    }
                                });

                            ui.add_space(6.0);
                            if ui.button("Submit").clicked() {
                                if let Some(ref res) = self.state.current_result {
                                    let ground_truth_label =
                                        self.state.selected_ground_truth.clone();
                                    let raw_data_to_send = res.raw_data.clone();
                                    let tx_event = self.state.tx_event.clone();

                                    thread::spawn(move || {
                                        let client = reqwest::blocking::Client::new();
                                        let mut sample_row = raw_data_to_send.clone();
                                        if sample_row.len() < 10 {
                                            sample_row.resize(10, 0.0);
                                        } else {
                                            sample_row.truncate(10);
                                        }
                                        let values: Vec<Vec<f32>> = vec![sample_row; 100];

                                        let payload_json = serde_json::json!({
                                            "protected": {
                                                "ver": "v1",
                                                "alg": "none"
                                            },
                                            "signature": "empty",
                                            "payload": {
                                                "device_name": "ESP32-S3",
                                                "device_type": "ESP32-S3",
                                                "interval_ms": 20,
                                                "sensors": [
                                                    { "name": "MQ3", "units": "raw" },
                                                    { "name": "MQ7", "units": "raw" },
                                                    { "name": "MQ135", "units": "raw" },
                                                    { "name": "MQ6", "units": "raw" },
                                                    { "name": "TGS2600", "units": "raw" },
                                                    { "name": "TGS2602", "units": "raw" },
                                                    { "name": "TGS2611", "units": "raw" },
                                                    { "name": "TGS2620", "units": "raw" },
                                                    { "name": "DHT_Temp", "units": "raw" },
                                                    { "name": "DHT_Hum", "units": "raw" }
                                                ],
                                                "values": values
                                            }
                                        });

                                        let response = client
                                            .post("https://ingestion.edgeimpulse.com/api/training/data")
                                            .header("x-api-key", "YOUR_EDGE_IMPULSE_API_KEY")
                                            .header("x-label", &ground_truth_label)
                                            .header("x-file-name", "enose_sample.json")
                                            .header("Content-Type", "application/json")
                                            .json(&payload_json)
                                            .send();

                                        match response {
                                            Ok(resp) => {
                                                if resp.status().is_success() {
                                                    let _ = tx_event.send(AppEvent::EdgeImpulseUploaded(Ok(format!(
                                                        "Sampel berhasil diunggah ke dataset Edge Impulse (Label: {})",
                                                        ground_truth_label
                                                    ))));
                                                } else {
                                                    let code = resp.status();
                                                    let body = resp.text().unwrap_or_default();
                                                    let _ = tx_event.send(AppEvent::EdgeImpulseUploaded(Err(format!(
                                                        "Gagal unggah Edge Impulse ({}): {}",
                                                        code, body
                                                    ))));
                                                }
                                            }
                                            Err(err) => {
                                                let _ = tx_event.send(AppEvent::EdgeImpulseUploaded(Err(format!(
                                                    "Koneksi Edge Impulse gagal: {}",
                                                    err
                                                ))));
                                            }
                                        }
                                    });

                                    self.state.test_records.push(TestRecord {
                                        timestamp: AppState::current_timestamp_str(),
                                        label: res.class.clone(),
                                        confidence: res.confidence,
                                        inference_time_ms: res.inference_time_ms,
                                        validation_status: "Misclassified".to_string(),
                                        ground_truth: self.state.selected_ground_truth.clone(),
                                    });
                                    self.state.current_result = None;
                                    self.state.misclassified_active = false;
                                    self.state.fsm_state = TestFsmState::Testing;
                                }
                            }
                        }
                    });
                },
            );

            ui.allocate_ui_with_layout(
                egui::vec2(col_width, ui.available_height()),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    Self::industrial_card_frame().show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.horizontal(|ui| {
                            ui.heading("Acquisition Records");
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                let has_records = !self.state.test_records.is_empty();
                                if ui
                                    .add_enabled(has_records, egui::Button::new("Clear All"))
                                    .clicked()
                                {
                                    self.state.test_records.clear();
                                }
                            });
                        });
                        ui.add_space(6.0);

                        let mut delete_idx = None;

                        egui::ScrollArea::vertical()
                            .auto_shrink([false, false])
                            .max_height(ui.available_height() - 20.0)
                            .show(ui, |ui| {
                                egui::Grid::new("records_table")
                                    .striped(true)
                                    .min_col_width(60.0)
                                    .spacing([12.0, 8.0])
                                    .show(ui, |ui| {
                                        ui.strong("No");
                                        ui.strong("Timestamp");
                                        ui.strong("Predicted Class");
                                        ui.strong("Confidence");
                                        ui.strong("Validation Status");
                                        ui.strong("Ground Truth");
                                        ui.strong("Action");
                                        ui.end_row();

                                        for (idx, item) in self.state.test_records.iter().enumerate() {
                                            ui.label(format!("{}", idx + 1));
                                            ui.label(&item.timestamp);
                                            ui.label(&item.label);
                                            ui.label(format!("{:.2}%", item.confidence));
                                            if item.validation_status == "Verified" {
                                                ui.colored_label(
                                                    egui::Color32::from_rgb(46, 204, 113),
                                                    "Verified",
                                                );
                                            } else {
                                                ui.colored_label(
                                                    egui::Color32::from_rgb(231, 76, 60),
                                                    "Misclassified",
                                                );
                                            }
                                            ui.label(&item.ground_truth);
                                            if ui.small_button("Delete").clicked() {
                                                delete_idx = Some(idx);
                                            }
                                            ui.end_row();
                                        }
                                    });
                            });

                        if let Some(idx) = delete_idx {
                            self.state.test_records.remove(idx);
                        }
                    });
                },
            );
        });
    }

    fn render_ota_view(&mut self, ui: &mut egui::Ui) {
        ui.vertical_centered(|ui| {
            ui.add_space(20.0);
            let frame = Self::industrial_card_frame();
            ui.allocate_ui_with_layout(
                egui::vec2(520.0, 360.0),
                egui::Layout::top_down(egui::Align::Center),
                |ui| {
                    frame.show(ui, |ui| {
                        ui.set_width(480.0);
                        ui.heading("Firmware OTA");
                        ui.add_space(14.0);

                        let is_uploading = self.state.ota_status == OtaStatus::Uploading;

                        if ui
                            .add_enabled(!is_uploading, egui::Button::new("Select .bin File"))
                            .clicked()
                        {
                            if let Some(path) = rfd::FileDialog::new()
                                .add_filter("Binary Firmware", &["bin"])
                                .pick_file()
                            {
                                if let Ok(bytes) = std::fs::read(&path) {
                                    self.state.selected_ota_path = Some(path);
                                    self.state.ota_file_data = Some(bytes);
                                    self.state.ota_status = OtaStatus::Idle;
                                    self.state.ota_progress = 0.0;
                                    self.state.ota_error_message = None;
                                }
                            }
                        }

                        ui.add_space(6.0);
                        if let Some(ref path) = self.state.selected_ota_path {
                            let fname = path.file_name().unwrap_or_default().to_string_lossy();
                            let bytes_count = self
                                .state
                                .ota_file_data
                                .as_ref()
                                .map(|d| d.len())
                                .unwrap_or(0);
                            ui.label(format!("File: {} ({} bytes)", fname, bytes_count));
                        } else {
                            ui.label(
                                egui::RichText::new("No firmware file selected")
                                    .color(egui::Color32::from_rgb(140, 150, 165)),
                            );
                        }

                        ui.add_space(14.0);
                        ui.label("Upload Progress");
                        let progress_bar = egui::ProgressBar::new(self.state.ota_progress)
                            .show_percentage()
                            .fill(egui::Color32::from_rgb(52, 152, 219))
                            .animate(is_uploading);
                        ui.add(progress_bar);

                        ui.add_space(8.0);
                        match self.state.ota_status {
                            OtaStatus::Idle => {
                                ui.label("Status: Ready");
                            }
                            OtaStatus::Uploading => {
                                ui.colored_label(
                                    egui::Color32::from_rgb(52, 152, 219),
                                    "Status: Transmitting 1024-byte payload chunks...",
                                );
                            }
                            OtaStatus::Success => {
                                ui.colored_label(
                                    egui::Color32::from_rgb(46, 204, 113),
                                    "Status: Flash completed successfully",
                                );
                            }
                            OtaStatus::Failed => {
                                let err = self
                                    .state
                                    .ota_error_message
                                    .clone()
                                    .unwrap_or_else(|| "Flash execution failed".to_string());
                                ui.colored_label(
                                    egui::Color32::from_rgb(231, 76, 60),
                                    format!("Status: {}", err),
                                );
                            }
                        }

                        ui.add_space(16.0);
                        let can_flash = self.state.ota_file_data.is_some()
                            && !is_uploading
                            && self.state.mqtt_connected;
                        if ui
                            .add_enabled(can_flash, egui::Button::new("Flash Firmware"))
                            .clicked()
                        {
                            self.start_ota_upload_process();
                        }
                    });
                },
            );
        });
    }
}

impl eframe::App for EdgeAiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.request_repaint_after(Duration::from_millis(50));

        if ctx.input(|i| i.key_pressed(egui::Key::F11)) {
            let current = ctx.input(|i| i.viewport().fullscreen.unwrap_or(false));
            ctx.send_viewport_cmd(egui::ViewportCommand::Fullscreen(!current));
        }

        while let Ok(event) = self.state.rx_event.try_recv() {
            match event {
                AppEvent::Connected => {
                    self.state.mqtt_connected = true;
                }
                AppEvent::Disconnected(_) => {
                    self.state.mqtt_connected = false;
                }
                AppEvent::ResultReceived(res) => {
                    if self.state.fsm_state == TestFsmState::Sampling {
                        self.state.current_result = Some(res);
                        self.state.sampling_start_time = None;
                        self.state.fsm_state = TestFsmState::Verification;
                        self.state.notification_msg = None;
                    }
                }
                AppEvent::OtaAckReceived => {}
                AppEvent::OtaProgress(prog) => {
                    self.state.ota_progress = prog;
                }
                AppEvent::OtaFinished(res) => match res {
                    Ok(()) => {
                        self.state.ota_status = OtaStatus::Success;
                        self.state.ota_progress = 1.0;
                    }
                    Err(e) => {
                        self.state.ota_status = OtaStatus::Failed;
                        self.state.ota_error_message = Some(e);
                    }
                },
                AppEvent::EdgeImpulseUploaded(res) => match res {
                    Ok(msg) => {
                        self.state.notification_msg = Some(msg);
                        self.state.notification_is_error = false;
                    }
                    Err(e) => {
                        self.state.notification_msg = Some(e);
                        self.state.notification_is_error = true;
                    }
                },
            }
        }

        if self.state.fsm_state == TestFsmState::Sampling {
            if let Some(start) = self.state.sampling_start_time {
                if start.elapsed() >= Duration::from_secs(10) {
                    self.state.fsm_state = TestFsmState::Testing;
                    self.state.sampling_start_time = None;
                    self.state.notification_msg =
                        Some("Device not responding within 10s timeout".to_string());
                    self.state.notification_is_error = true;
                }
            }
        }

        let nav_disabled = self.state.fsm_state != TestFsmState::Idle
            || self.state.ota_status == OtaStatus::Uploading;

        egui::TopBottomPanel::top("header_panel")
            .frame(
                egui::Frame::none()
                    .fill(egui::Color32::from_rgb(18, 20, 26))
                    .inner_margin(egui::Margin::symmetric(14.0, 8.0)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if let Some(ref tex) = self.logo_texture {
                        ui.image((tex.id(), egui::vec2(26.0, 26.0)));
                    }
                    ui.label(
                        egui::RichText::new("GUI E-Nose")
                            .strong()
                            .size(17.0)
                            .color(egui::Color32::WHITE),
                    );
                    ui.separator();

                    ui.add_enabled_ui(!nav_disabled, |ui| {
                        if ui
                            .selectable_label(
                                self.state.current_page == AppPage::Testing,
                                "Testing",
                            )
                            .clicked()
                        {
                            self.state.current_page = AppPage::Testing;
                        }
                        if ui
                            .selectable_label(
                                self.state.current_page == AppPage::FirmwareOta,
                                "Firmware OTA",
                            )
                            .clicked()
                        {
                            self.state.current_page = AppPage::FirmwareOta;
                        }
                    });

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if self.state.mqtt_connected {
                            ui.colored_label(
                                egui::Color32::from_rgb(46, 204, 113),
                                "● Broker Connected",
                            );
                        } else {
                            ui.colored_label(
                                egui::Color32::from_rgb(231, 76, 60),
                                "○ Broker Disconnected",
                            );
                        }
                    });
                });
            });

        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(egui::Color32::from_rgb(14, 16, 20))
                    .inner_margin(egui::Margin::same(12.0)),
            )
            .show(ctx, |ui| {
                let mut close_notification = false;
                if let Some(ref msg) = self.state.notification_msg {
                    let color = if self.state.notification_is_error {
                        egui::Color32::from_rgb(231, 76, 60)
                    } else {
                        egui::Color32::from_rgb(46, 204, 113)
                    };
                    egui::Frame::none()
                        .fill(egui::Color32::from_rgb(28, 30, 36))
                        .stroke(egui::Stroke::new(1.0_f32, color))
                        .inner_margin(8.0)
                        .rounding(4.0)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.colored_label(color, msg);
                                if ui.button("Dismiss").clicked() {
                                    close_notification = true;
                                }
                            });
                        });
                    ui.add_space(8.0);
                }
                if close_notification {
                    self.state.notification_msg = None;
                }

                match self.state.current_page {
                    AppPage::Testing => self.render_testing_view(ui),
                    AppPage::FirmwareOta => self.render_ota_view(ui),
                }
            });
    }
}

fn main() -> eframe::Result<()> {
    let icon_data = if let Ok(img) = ::image::load_from_memory(include_bytes!("../assets/logo.png")) {
        let (w, h) = (img.width(), img.height());
        Some(egui::IconData {
            rgba: img.to_rgba8().into_raw(),
            width: w,
            height: h,
        })
    } else {
        None
    };

    let mut viewport = egui::ViewportBuilder::default()
        .with_inner_size([1920.0, 1080.0])
        .with_min_inner_size([1920.0, 1080.0])
        .with_resizable(false)
        .with_title("GUI E-Nose");

    if let Some(icon) = icon_data {
        viewport = viewport.with_icon(icon);
    }

    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        "GUI E-Nose",
        options,
        Box::new(|cc| Ok(Box::new(EdgeAiApp::new(cc)))),
    )
}