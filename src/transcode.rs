use axum::{body::Bytes, BoxError};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::sleep;

const PROBE_SIZE: usize = 10 * 1024 * 1024;
const MAX_AUDIO_TRACKS: usize = 8;
const TEXT_SUBTITLE_CODECS: &[&str] = &["subrip", "srt", "ass", "ssa", "mov_text", "webvtt", "text"];
const MASTER_PLAYLIST: &str = "stream.m3u8";
const TRACKS_FILE: &str = "tracks.json";
const SUBTITLE_PART_SUFFIX: &str = ".part";
// const SUBTITLE_PUBLISH_INTERVAL: u64 = 2;

lazy_static::lazy_static! {
    static ref ISO639_2_TO_1: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        let mapping = [
            ("en", vec!["eng"]), ("fr", vec!["fre", "fra"]), ("de", vec!["ger", "deu"]),
            ("es", vec!["spa"]), ("it", vec!["ita"]), ("pt", vec!["por"]),
            ("ru", vec!["rus"]), ("ja", vec!["jpn"]), ("ko", vec!["kor"]),
            ("zh", vec!["chi", "zho"]), ("hi", vec!["hin"]), ("ar", vec!["ara"]),
            ("nl", vec!["dut", "nld"]), ("sv", vec!["swe"]), ("no", vec!["nor"]),
            ("da", vec!["dan"]), ("fi", vec!["fin"]), ("pl", vec!["pol"]),
            ("tr", vec!["tur"]), ("cs", vec!["cze", "ces"]), ("el", vec!["gre", "ell"]),
            ("he", vec!["heb"]), ("th", vec!["tha"]), ("uk", vec!["ukr"]),
            ("hu", vec!["hun"]), ("ro", vec!["rum", "ron"]),
        ];
        for (lang, codes) in mapping {
            for code in codes { map.insert(code, lang); }
        }
        map
    };
}

#[derive(Debug, Deserialize, Serialize)]
struct FFProbeStream {
    index: i32,
    codec_type: String,
    codec_name: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    #[serde(default)]
    disposition: HashMap<String, i32>,
    channels: Option<i32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AudioTrack {
    pub stream: i32,
    pub language: String,
    pub title: String,
    pub channels: i32,
    pub default: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct SubtitleTrack {
    pub stream: i32,
    pub language: String,
    pub title: String,
    pub forced: bool,
    pub file: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct Tracks {
    pub audio: Vec<AudioTrack>,
    pub subtitles: Vec<SubtitleTrack>,
}

async fn probe_streams(data: &[u8]) -> Result<Vec<FFProbeStream>, Box<dyn std::error::Error>> {
    let mut child = Command::new("ffprobe")
        .args(["-v", "quiet", "-print_format", "json", "-show_streams", "pipe:0"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(data).await?;
    drop(stdin);

    let output = child.wait_with_output().await?;
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let streams: Vec<FFProbeStream> = serde_json::from_value(json["streams"].clone())?;
    Ok(streams)
}

fn get_language(stream: &FFProbeStream) -> String {
    let lang = stream.tags.get("language").cloned().unwrap_or_default().to_lowercase();
    if lang == "und" || lang == "unk" || lang.is_empty() {
        return "".to_string();
    }
    ISO639_2_TO_1.get(lang.as_str()).map(|s| s.to_string()).unwrap_or(lang)
}

fn select_tracks(streams: &[FFProbeStream], preferred_lang: &str) -> Tracks {
    let mut audio: Vec<AudioTrack> = streams.iter()
        .filter(|s| s.codec_type == "audio")
        .map(|s| AudioTrack {
            stream: s.index,
            language: get_language(s),
            title: s.tags.get("title").cloned().unwrap_or_default(),
            channels: s.channels.unwrap_or(0),
            default: false,
        })
        .take(MAX_AUDIO_TRACKS)
        .collect();

    let pref = preferred_lang.trim().to_lowercase();
    let default_idx = if !pref.is_empty() {
        audio.iter().position(|t| t.language == pref)
    } else {
        None
    };
    if let Some(track) = audio.get_mut(default_idx.unwrap_or(0)) {
        track.default = true;
    }

    let subtitles = streams.iter()
        .filter(|s| s.codec_type == "subtitle" && TEXT_SUBTITLE_CODECS.contains(&s.codec_name.as_str()))
        .enumerate()
        .map(|(i, s)| SubtitleTrack {
            stream: s.index,
            language: get_language(s),
            title: s.tags.get("title").cloned().unwrap_or_default(),
            forced: s.disposition.get("forced").copied().unwrap_or(0) != 0,
            file: format!("sub_{}.vtt", i),
        })
        .collect();

    Tracks { audio, subtitles }
}

fn video_args(codec: &str) -> Vec<String> {
    match codec {
        "h264" => vec!["-c:v".into(), "copy".into(), "-bsf:v".into(), "h264_mp4toannexb".into()],
        "hevc" => vec!["-c:v".into(), "copy".into(), "-bsf:v".into(), "hevc_mp4toannexb".into()],
        _ => vec![
            "-c:v".into(), "h264_videotoolbox".into(), "-b:v".into(), "5M".into(),
            "-maxrate".into(), "7M".into(), "-bufsize".into(), "10M".into(),
            "-bsf:v".into(), "h264_mp4toannexb".into()
        ],
    }
}

async fn write_atomic(path: &Path, content: &[u8]) -> std::io::Result<()> {
    let mut tmp_path = path.to_path_buf();
    tmp_path.set_extension("tmp");
    fs::write(&tmp_path, content).await?;
    fs::rename(tmp_path, path).await
}

fn build_master_playlist(tracks: &Tracks) -> String {
    let mut lines = vec!["#EXTM3U".to_string(), "#EXT-X-VERSION:4".to_string()];
    for (i, track) in tracks.audio.iter().enumerate() {
        let name = if !track.language.is_empty() || !track.title.is_empty() {
            format!("{}. {} - {}", i + 1, track.language, track.title)
        } else {
            format!("{}. audio", i + 1)
        }.replace('"', "'");

        let mut attr = vec![
            "TYPE=AUDIO".to_string(),
            "GROUP-ID=\"audio\"".to_string(),
            format!("NAME=\"{}\"", name),
        ];
        if !track.language.is_empty() { attr.push(format!("LANGUAGE=\"{}\"", track.language)); }
        attr.push(format!("DEFAULT={}", if track.default { "YES" } else { "NO" }));
        attr.push("AUTOSELECT=YES".into());
        attr.push(format!("URI=\"track_{}.m3u8\"", i + 1));
        lines.push(format!("#EXT-X-MEDIA:{}", attr.join(",")));
    }
    lines.push(format!("#EXT-X-STREAM-INF:BANDWIDTH=5000000{}", if !tracks.audio.is_empty() { ",AUDIO=\"audio\"" } else { "" }));
    lines.push("track_0.m3u8".into());
    lines.join("\n") + "\n"
}

pub async fn convert_pipe_hls<S, E>(
    mut stream: S,
    output_dir: PathBuf,
    preferred_lang: String,
) -> Result<Tracks, BoxError>
where
    E: Into<BoxError>,
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut head = Vec::with_capacity(PROBE_SIZE);
    while head.len() < PROBE_SIZE {
        if let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Into::into)?;
            head.extend_from_slice(&chunk);
        } else { break; }
    }

    let mut tracks = Tracks { audio: vec![], subtitles: vec![] };
    let mut ffmpeg_args = vec![];
    let mut is_legacy = false;

    match probe_streams(&head).await {
        Ok(streams) => {
            let v_codec = streams.iter().find(|s| s.codec_type == "video").map(|s| s.codec_name.as_str()).unwrap_or("");
            if v_codec.is_empty() { return Err("no video stream".into()); }
            tracks = select_tracks(&streams, &preferred_lang);

            ffmpeg_args.extend([ "-i".into(), "pipe:0".into(), "-map".into(), "0:v:0".into()]);
            let mut variants = vec![if tracks.audio.is_empty() { "v:0".into() } else { "v:0,agroup:audio".into() }];
            for (i, t) in tracks.audio.iter().enumerate() {
                ffmpeg_args.extend(["-map".into(), format!("0:{}", t.stream)]);
                variants.push(format!("a:{},agroup:audio", i));
            }
            ffmpeg_args.extend(video_args(v_codec));
            if !tracks.audio.is_empty() {
                ffmpeg_args.extend(["-c:a".into(), "aac".into(), "-b:a".into(), "256k".into(), "-ac".into(), "2".into()]);
            }
            ffmpeg_args.extend(["-avoid_negative_ts".into(), "make_zero".into(), "-hls_time".into(), "5".into(), "-hls_list_size".into(), "0".into(), "-hls_flags".into(), "append_list+temp_file".into()]);
            ffmpeg_args.extend(["-var_stream_map".into(), variants.join(" "), "-hls_segment_filename".into(), output_dir.join("track_%v_%05d.ts").to_string_lossy().into(), "-f".into(), "hls".into(), output_dir.join("track_%v.m3u8").to_string_lossy().into()]);

            for t in &tracks.subtitles {
                ffmpeg_args.extend(["-map".into(), format!("0:{}", t.stream), "-c:s".into(), "webvtt".into(), "-flush_packets".into(), "1".into(), "-f".into(), "webvtt".into(), output_dir.join(format!("{}{}", t.file, SUBTITLE_PART_SUFFIX)).to_string_lossy().into()]);
            }
        }
        Err(_) => {
            is_legacy = true;
            ffmpeg_args.extend(["-i".into(), "pipe:0".into(), "-map".into(), "0:v:0".into(), "-map".into(), "0:a:0?".into()]);
            ffmpeg_args.extend(video_args(""));
            ffmpeg_args.extend(["-c:a".into(), "aac".into(), "-b:a".into(), "256k".into(), "-ac".into(), "2".into()]);
            ffmpeg_args.extend(["-avoid_negative_ts".into(), "make_zero".into(), "-hls_time".into(), "5".into(), "-hls_list_size".into(), "0".into(), "-hls_flags".into(), "append_list+temp_file".into(), "-f".into(), "hls".into(), output_dir.join(MASTER_PLAYLIST).to_string_lossy().into()]);
        }
    }

    fs::write(output_dir.join(TRACKS_FILE), serde_json::to_string(&tracks)?).await?;

    let mut child = Command::new("ffmpeg")
        .args(ffmpeg_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&head).await?;

    let out_dir_clone = output_dir.clone();
    let tracks_clone = tracks.clone();

    let monitor_handle = tokio::spawn(async move {
        let playlists: Vec<_> = (0..=tracks_clone.audio.len()).map(|i| out_dir_clone.join(format!("track_{}.m3u8", i))).collect();
        let mut sub_sizes: HashMap<String, usize> = HashMap::new();

        loop {
            if !is_legacy {
                let mut all_exist = true;
                for p in &playlists { if !p.exists() { all_exist = false; break; } }
                if all_exist {
                    let _ = write_atomic(&out_dir_clone.join(MASTER_PLAYLIST), build_master_playlist(&tracks_clone).as_bytes()).await;
                }

                for t in &tracks_clone.subtitles {
                    let part_path = out_dir_clone.join(format!("{}{}", t.file, SUBTITLE_PART_SUFFIX));
                    if let Ok(data) = fs::read(&part_path).await {
                        if sub_sizes.get(&t.file) != Some(&data.len()) {
                            sub_sizes.insert(t.file.clone(), data.len());
                            let _ = write_atomic(&out_dir_clone.join(&t.file), &data).await;
                        }
                    }
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    });

    let mut body_stream = stream;
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.map_err(Into::into)?;
        if stdin.write_all(&chunk).await.is_err() { break; }
    }    drop(stdin);

    let status = child.wait().await?;
    monitor_handle.abort();

    if !status.success() {
        let mut err_str = String::new();
        if let Some(mut stderr) = child.stderr.take() { stderr.read_to_string(&mut err_str).await?; }
        return Err(format!("ffmpeg failed: {}\n{}", status, err_str).into());
    }

    Ok(tracks)
}
