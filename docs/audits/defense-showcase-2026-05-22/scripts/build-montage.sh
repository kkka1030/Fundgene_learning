#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/xueyicheng/Documents/SRTP/FundGene/docs/audits/defense-showcase-2026-05-22"
MP4_DIR="$ROOT_DIR/assets/mp4"
FINAL_DIR="$ROOT_DIR/assets/final"
TMP_DIR="$ROOT_DIR/.montage-tmp"
FONT_MEDIUM="/System/Library/Fonts/STHeiti Medium.ttc"
FONT_LIGHT="/System/Library/Fonts/STHeiti Light.ttc"
NORMALIZE_CRF="${NORMALIZE_CRF:-18}"
FINAL_CRF="${FINAL_CRF:-18}"
VIDEO_PRESET="${VIDEO_PRESET:-slow}"
XFADE_DURATION="${XFADE_DURATION:-0.28}"
CHAPTER_DURATION="${CHAPTER_DURATION:-1.28}"
OUTRO_DURATION="${OUTRO_DURATION:-2.15}"

mkdir -p "$FINAL_DIR"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

extract_frame() {
  local input="$1"
  local output="$2"
  local timestamp="${3:-00:00:01.20}"

  ffmpeg -nostdin -y -ss "$timestamp" -i "$input" -frames:v 1 -update 1 "$output"
}

make_slate_png() {
  local index="$1"
  local title="$2"
  local subtitle="$3"
  local output="$4"
  local bg="${5:-}"

  local base_args=(-size 1440x980 gradient:'#f7fbff-#eef7ff')
  if [[ -n "$bg" && -f "$bg" ]]; then
    base_args=("$bg" -resize '1440x980^' -gravity center -extent 1440x980 -blur 0x13)
  fi

  magick "${base_args[@]}" \
    -fill 'rgba(246,251,255,0.76)' -draw 'rectangle 0,0 1440,980' \
    -fill 'rgba(0,113,227,0.050)' -draw 'circle 1138,164 1588,614' \
    -fill 'rgba(70,208,172,0.036)' -draw 'circle 280,858 638,1216' \
    -fill 'rgba(255,255,255,0.80)' -draw 'roundrectangle 118,120 1322,860 44,44' \
    -stroke 'rgba(15,23,42,0.065)' -strokewidth 2 -fill none -draw 'roundrectangle 118,120 1322,860 44,44' \
    -fill 'rgba(255,255,255,0.56)' -draw 'roundrectangle 848,188 1224,724 34,34' \
    -stroke 'rgba(255,255,255,0.76)' -strokewidth 2 -fill none -draw 'roundrectangle 848,188 1224,724 34,34' \
    -stroke 'rgba(0,113,227,0.34)' -strokewidth 5 -draw 'line 174,680 502,680' \
    -stroke 'rgba(15,23,42,0.075)' -strokewidth 1 -draw 'line 174,724 502,724' \
    -stroke none \
    -fill '#0071e3' -draw 'roundrectangle 174,178 272,276 28,28' \
    -font "$FONT_MEDIUM" -fill '#ffffff' -pointsize 30 -gravity northwest -annotate +204+212 "$index" \
    -font "$FONT_LIGHT" -fill '#64748b' -pointsize 22 -gravity northwest -annotate +174+344 'FundGene' \
    -font "$FONT_MEDIUM" -fill '#0f172a' -pointsize 78 -gravity northwest -annotate +174+424 "$title" \
    -font "$FONT_LIGHT" -fill '#334155' -pointsize 30 -gravity northwest -annotate +178+532 "$subtitle" \
    -font "$FONT_LIGHT" -fill '#94a3b8' -pointsize 21 -gravity northwest -annotate +178+758 'Explain · Train · Confirm' \
    -font "$FONT_LIGHT" -fill 'rgba(15,23,42,0.23)' -pointsize 22 -gravity northwest -annotate +912+280 '理解变化' \
    -font "$FONT_LIGHT" -fill 'rgba(15,23,42,0.23)' -pointsize 22 -gravity northwest -annotate +912+408 '练习判断' \
    -font "$FONT_LIGHT" -fill 'rgba(15,23,42,0.23)' -pointsize 22 -gravity northwest -annotate +912+536 '确认行动' \
    -stroke 'rgba(0,113,227,0.22)' -strokewidth 4 -draw 'line 894,320 1178,320' \
    -stroke 'rgba(70,208,172,0.18)' -strokewidth 4 -draw 'line 894,448 1178,448' \
    -stroke 'rgba(255,149,0,0.16)' -strokewidth 4 -draw 'line 894,576 1178,576' \
    "$output"
}

make_intro_png() {
  local output="$1"
  local bg="$2"

  magick "$bg" -resize '1440x980^' -gravity center -extent 1440x980 -blur 0x18 \
    -fill 'rgba(246,251,255,0.72)' -draw 'rectangle 0,0 1440,980' \
    -fill 'rgba(255,255,255,0.82)' -draw 'roundrectangle 118,116 1322,864 46,46' \
    -stroke 'rgba(15,23,42,0.075)' -strokewidth 2 -fill none -draw 'roundrectangle 118,116 1322,864 46,46' \
    -stroke none -fill '#0071e3' -draw 'roundrectangle 184,190 288,294 30,30' \
    -font "$FONT_MEDIUM" -fill '#ffffff' -pointsize 30 -gravity northwest -annotate +217+224 'FG' \
    -font "$FONT_LIGHT" -fill '#64748b' -pointsize 25 -gravity northwest -annotate +184+382 'FundGene · Agent Command Center' \
    -font "$FONT_MEDIUM" -fill '#0f172a' -pointsize 74 -gravity northwest -annotate +184+462 '先理解，再决策' \
    -font "$FONT_LIGHT" -fill '#334155' -pointsize 32 -gravity northwest -annotate +188+570 '每天先给基金新手一个可解释、可确认的判断' \
    -stroke 'rgba(0,113,227,0.34)' -strokewidth 5 -draw 'line 188,652 560,652' \
    -font "$FONT_LIGHT" -fill '#64748b' -pointsize 24 -gravity northwest -annotate +190+728 'Explain · Train · Confirm' \
    "$output"
}

make_outro_png() {
  local output="$1"

  magick -size 1440x980 gradient:'#f8fbff-#edf7ff' \
    -fill 'rgba(0,113,227,0.060)' -draw 'circle 1120,170 1600,650' \
    -fill 'rgba(70,208,172,0.052)' -draw 'circle 268,838 668,1238' \
    -fill 'rgba(255,255,255,0.84)' -draw 'roundrectangle 150,144 1290,836 42,42' \
    -stroke 'rgba(15,23,42,0.075)' -strokewidth 2 -fill none -draw 'roundrectangle 150,144 1290,836 42,42' \
    -stroke none \
    -fill '#0071e3' -draw 'roundrectangle 206,204 310,308 30,30' \
    -font "$FONT_MEDIUM" -fill '#ffffff' -pointsize 30 -gravity northwest -annotate +239+238 'FG' \
    -font "$FONT_MEDIUM" -fill '#0f172a' -pointsize 86 -gravity northwest -annotate +206+420 'FundGene' \
    -font "$FONT_LIGHT" -fill '#334155' -pointsize 38 -gravity northwest -annotate +211+526 'Explain · Train · Confirm' \
    -font "$FONT_LIGHT" -fill '#64748b' -pointsize 26 -gravity northwest -annotate +212+650 '让基金新手先理解，再决策。' \
    -stroke 'rgba(0,113,227,0.30)' -strokewidth 5 -draw 'line 212,594 548,594' \
    "$output"
}

make_slate_video() {
  local index="$1"
  local title="$2"
  local subtitle="$3"
  local duration="$4"
  local output="$5"
  local bg="${6:-}"
  local png="$TMP_DIR/slate-$index.png"

  make_slate_png "$index" "$title" "$subtitle" "$png" "$bg"
  ffmpeg -nostdin -y -loop 1 -t "$duration" -i "$png" \
    -vf "fps=30,scale=1440:980,zoompan=z='min(zoom+0.00045,1.018)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1440x980:fps=30,format=yuv420p,setsar=1" \
    -c:v libx264 -preset veryfast -crf 18 \
    "$output"
}

make_intro_video() {
  local duration="$1"
  local output="$2"
  local bg="$3"
  local png="$TMP_DIR/slate-intro.png"

  make_intro_png "$png" "$bg"
  ffmpeg -nostdin -y -loop 1 -t "$duration" -i "$png" \
    -vf "fps=30,scale=1440:980,zoompan=z='min(zoom+0.00036,1.014)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1440x980:fps=30,format=yuv420p,setsar=1" \
    -c:v libx264 -preset veryfast -crf 18 \
    "$output"
}

make_outro_video() {
  local duration="$1"
  local output="$2"
  local png="$TMP_DIR/slate-outro.png"

  make_outro_png "$png"
  ffmpeg -nostdin -y -loop 1 -t "$duration" -i "$png" \
    -vf "fps=30,scale=1440:980,zoompan=z='min(zoom+0.00042,1.017)':d=1:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1440x980:fps=30,format=yuv420p,setsar=1" \
    -c:v libx264 -preset veryfast -crf 18 \
    "$output"
}

camera_clip() {
  local input="$1"
  local output="$2"

  ffmpeg -nostdin -y -i "$input" \
    -vf "fps=30,scale=1440:980:force_original_aspect_ratio=decrease,pad=1440:980:(ow-iw)/2:(oh-ih)/2,format=yuv420p,setsar=1" \
    -an -c:v libx264 -preset "$VIDEO_PRESET" -crf "$NORMALIZE_CRF" \
    "$output"
}

extract_frame "$MP4_DIR/01-hero-command-center.mp4" "$TMP_DIR/bg-workspace.png" "00:00:01.20"
extract_frame "$MP4_DIR/02-today-daily-brief.mp4" "$TMP_DIR/bg-today.png" "00:00:01.20"
extract_frame "$MP4_DIR/03-agent-workspace.mp4" "$TMP_DIR/bg-agent.png" "00:00:03.80"
extract_frame "$MP4_DIR/04-news-impact.mp4" "$TMP_DIR/bg-news.png" "00:00:02.40"
extract_frame "$MP4_DIR/05-simulation-training.mp4" "$TMP_DIR/bg-simulation.png" "00:00:03.20"
extract_frame "$MP4_DIR/06-automation-profile.mp4" "$TMP_DIR/bg-automation.png" "00:00:04.20"

make_intro_video "$CHAPTER_DURATION" "$TMP_DIR/00-intro.mp4" "$TMP_DIR/bg-workspace.png"
make_slate_video "01" "进入工作区" "核心任务，一屏展开" "$CHAPTER_DURATION" "$TMP_DIR/01-workspace-slate.mp4" "$TMP_DIR/bg-workspace.png"
camera_clip "$MP4_DIR/01-hero-command-center.mp4" "$TMP_DIR/02-workspace.mp4"
make_slate_video "02" "先给判断" "今天该关注什么" "$CHAPTER_DURATION" "$TMP_DIR/03-today-slate.mp4" "$TMP_DIR/bg-today.png"
camera_clip "$MP4_DIR/02-today-daily-brief.mp4" "$TMP_DIR/04-today.mp4"
make_slate_video "03" "解释影响" "结合画像与组合" "$CHAPTER_DURATION" "$TMP_DIR/05-agent-slate.mp4" "$TMP_DIR/bg-agent.png"
camera_clip "$MP4_DIR/03-agent-workspace.mp4" "$TMP_DIR/06-agent.mp4"
make_slate_video "04" "读懂资讯" "新闻进入个人组合" "$CHAPTER_DURATION" "$TMP_DIR/07-news-slate.mp4" "$TMP_DIR/bg-news.png"
camera_clip "$MP4_DIR/04-news-impact.mp4" "$TMP_DIR/08-news.mp4"
make_slate_video "05" "训练判断" "在历史情境里练习" "$CHAPTER_DURATION" "$TMP_DIR/09-simulation-slate.mp4" "$TMP_DIR/bg-simulation.png"
camera_clip "$MP4_DIR/05-simulation-training.mp4" "$TMP_DIR/10-simulation.mp4"
make_slate_video "06" "持续沉淀" "观察、确认、回写" "$CHAPTER_DURATION" "$TMP_DIR/11-automation-slate.mp4" "$TMP_DIR/bg-automation.png"
camera_clip "$MP4_DIR/06-automation-profile.mp4" "$TMP_DIR/12-automation.mp4"
make_outro_video "$OUTRO_DURATION" "$TMP_DIR/13-outro.mp4"

clips=(
  "$TMP_DIR/00-intro.mp4"
  "$TMP_DIR/01-workspace-slate.mp4"
  "$TMP_DIR/02-workspace.mp4"
  "$TMP_DIR/03-today-slate.mp4"
  "$TMP_DIR/04-today.mp4"
  "$TMP_DIR/05-agent-slate.mp4"
  "$TMP_DIR/06-agent.mp4"
  "$TMP_DIR/07-news-slate.mp4"
  "$TMP_DIR/08-news.mp4"
  "$TMP_DIR/09-simulation-slate.mp4"
  "$TMP_DIR/10-simulation.mp4"
  "$TMP_DIR/11-automation-slate.mp4"
  "$TMP_DIR/12-automation.mp4"
  "$TMP_DIR/13-outro.mp4"
)

MONTAGE_MP4="$FINAL_DIR/fundgene-defense-demo.mp4"
MONTAGE_GIF="$FINAL_DIR/fundgene-defense-demo-preview.gif"
MONTAGE_POSTER="$FINAL_DIR/fundgene-defense-demo-poster.png"
PALETTE="$TMP_DIR/montage-palette.png"

input_args=()
durations=()
for clip in "${clips[@]}"; do
  input_args+=(-i "$clip")
  durations+=("$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$clip")")
done

filter_complex=""
for index in "${!clips[@]}"; do
  filter_complex+="[$index:v]fps=30,format=yuv420p,settb=AVTB[v$index];"
done

last_label="[v0]"
cumulative="${durations[0]}"
for ((index = 1; index < ${#clips[@]}; index++)); do
  offset="$(python3 - "$cumulative" "$XFADE_DURATION" "$index" <<'PY'
import sys
cumulative = float(sys.argv[1])
xfade = float(sys.argv[2])
index = int(sys.argv[3])
print(f"{cumulative - xfade * index:.6f}")
PY
)"
  out_label="[x$index]"
  filter_complex+="$last_label[v$index]xfade=transition=fade:duration=$XFADE_DURATION:offset=$offset$out_label;"
  last_label="$out_label"
  cumulative="$(python3 - "$cumulative" "${durations[$index]}" <<'PY'
import sys
print(f"{float(sys.argv[1]) + float(sys.argv[2]):.6f}")
PY
)"
done
filter_complex="${filter_complex%;}"

ffmpeg -nostdin -y "${input_args[@]}" \
  -filter_complex "$filter_complex" -map "$last_label" \
  -c:v libx264 -preset "$VIDEO_PRESET" -crf "$FINAL_CRF" -pix_fmt yuv420p -movflags +faststart \
  "$MONTAGE_MP4"

ffmpeg -nostdin -y -ss 00:00:02 -i "$MONTAGE_MP4" -frames:v 1 -update 1 "$MONTAGE_POSTER"

ffmpeg -nostdin -y -i "$MONTAGE_MP4" \
  -vf "fps=15,scale=900:-1:flags=lanczos,palettegen" \
  -update 1 "$PALETTE"
ffmpeg -nostdin -y -i "$MONTAGE_MP4" -i "$PALETTE" \
  -lavfi "fps=15,scale=900:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3" \
  "$MONTAGE_GIF"

rm -rf "$TMP_DIR"

echo "created $MONTAGE_MP4"
echo "created $MONTAGE_GIF"
