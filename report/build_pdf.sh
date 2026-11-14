#!/usr/bin/env bash
# Builds TECHNICAL_REPORT.pdf from TECHNICAL_REPORT.md (needs pandoc + Chrome/Chromium)
set -euo pipefail
cd "$(dirname "$0")"
pandoc TECHNICAL_REPORT.md -s --embed-resources --css report.css --resource-path=. -o TECHNICAL_REPORT.html
CHROME=$(command -v google-chrome || command -v chromium || command -v chromium-browser)
"$CHROME" --headless --no-sandbox --disable-gpu --no-pdf-header-footer \
  --print-to-pdf=TECHNICAL_REPORT.pdf TECHNICAL_REPORT.html 2>/dev/null
rm TECHNICAL_REPORT.html
echo "wrote report/TECHNICAL_REPORT.pdf"
