#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/xueyicheng/Documents/SRTP/FundGene/docs/audits/defense-showcase-2026-05-22"
RAW_DIR="${1:-$ROOT_DIR/raw}"
MP4_DIR="$ROOT_DIR/assets/mp4"
GIF_DIR="$ROOT_DIR/assets/gif"
PNG_DIR="$ROOT_DIR/assets/png"
TRIM_START="${TRIM_START:-0.85}"
GIF_WIDTH="${GIF_WIDTH:-1120}"
GIF_FPS="${GIF_FPS:-20}"
MP4_CRF="${MP4_CRF:-18}"
MP4_PRESET="${MP4_PRESET:-slow}"

mkdir -p "$MP4_DIR" "$GIF_DIR" "$PNG_DIR"

asset_name_for_path() {
  local path="$1"
  case "$path" in
    *01-hero-command-center*) echo "01-hero-command-center" ;;
    *02-today-daily-brief*) echo "02-today-daily-brief" ;;
    *03-agent-workspace*) echo "03-agent-workspace" ;;
    *04-news-impact*) echo "04-news-impact" ;;
    *05-simulation-training*) echo "05-simulation-training" ;;
    *06-automation-profile*) echo "06-automation-profile" ;;
    *) basename "$(dirname "$path")" | tr '[:upper:] ' '[:lower:]-' ;;
  esac
}

find "$RAW_DIR" -name '*.webm' -print0 | while IFS= read -r -d '' input; do
  name="$(asset_name_for_path "$input")"
  mp4="$MP4_DIR/$name.mp4"
  gif="$GIF_DIR/$name.gif"
  palette="$PNG_DIR/$name-palette.png"
  poster="$PNG_DIR/$name-poster.png"

  ffmpeg -nostdin -y -ss "$TRIM_START" -i "$input" \
    -vf "scale=1440:-2,fps=30" \
    -c:v libx264 -preset "$MP4_PRESET" -crf "$MP4_CRF" -pix_fmt yuv420p -movflags +faststart \
    "$mp4"

  ffmpeg -nostdin -y -ss 00:00:00.40 -i "$mp4" -frames:v 1 -update 1 "$poster"

  ffmpeg -nostdin -y -i "$mp4" \
    -vf "fps=$GIF_FPS,scale=$GIF_WIDTH:-1:flags=lanczos,palettegen" \
    -update 1 "$palette"
  ffmpeg -nostdin -y -i "$mp4" -i "$palette" \
    -lavfi "fps=$GIF_FPS,scale=$GIF_WIDTH:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3" \
    "$gif"

  echo "converted $name"
done

if compgen -G "$PNG_DIR/*-poster.png" > /dev/null; then
  magick "$PNG_DIR"/*-poster.png \
    -resize 360x245^ -gravity center -extent 360x245 \
    -background white -splice 0x24 \
    +append "$PNG_DIR/showcase-contact-sheet.png"
fi
