set dotenv-load := false

# List available commands.
default:
    @just --list

# Download Pdfium binaries. Optionally pass the release tag, e.g. `just download-pdfium chromium/7483`.
download-pdfium *args:
    scripts/download_pdfium.sh {{args}}
