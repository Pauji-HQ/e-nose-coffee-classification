# bump_and_build.ps1
# Jalankan dari folder proyek (C:\e\hive_ota_tb), atau dari mana saja dengan
# -ProjectDir menunjuk ke folder itu.
#
# Langkah: baca FW_VERSION di main.rs -> naikkan angka terakhir (patch)
#          -> tulis balik -> cargo build --release -> espflash save-image
#          -> firmware.bin
#
# Pemakaian:
#   .\bump_and_build.ps1
#   .\bump_and_build.ps1 -ProjectDir C:\e\hive_ota_tb

param(
    [string]$ProjectDir = (Get-Location).Path
)

$ErrorActionPreference = "Stop"

$MainRs = Join-Path $ProjectDir "src\bin\main.rs"
if (-not (Test-Path $MainRs)) {
    Write-Error "Tidak ketemu $MainRs. Jalankan skrip ini dari folder proyek, atau pakai -ProjectDir."
    exit 1
}

# ---------- 1) Baca versi sekarang ----------
$content = Get-Content -Path $MainRs -Raw
$pattern = 'const FW_VERSION: &str = "(\d+)\.(\d+)\.(\d+)";'
$match = [regex]::Match($content, $pattern)

if (-not $match.Success) {
    Write-Error "Tidak ketemu baris 'const FW_VERSION: &str = ""X.Y.Z"";' di main.rs. Cek formatnya belum berubah."
    exit 1
}

$major = [int]$match.Groups[1].Value
$minor = [int]$match.Groups[2].Value
$patch = [int]$match.Groups[3].Value
$oldVersion = "$major.$minor.$patch"

# ---------- 2) Naikkan angka terakhir (patch) ----------
$patch += 1
$newVersion = "$major.$minor.$patch"

Write-Host "Versi sekarang : $oldVersion"
Write-Host "Versi baru     : $newVersion"

# ---------- 3) Tulis balik ke main.rs ----------
$newLine = "const FW_VERSION: &str = `"$newVersion`";"
$newContent = $content -replace $pattern, $newLine
Set-Content -Path $MainRs -Value $newContent -NoNewline

Write-Host "main.rs sudah diperbarui: FW_VERSION = `"$newVersion`""

# ---------- 4) Build release ----------
Push-Location $ProjectDir
try {
    Write-Host "`nMenjalankan cargo build --release ..."
    cargo build --release
    if ($LASTEXITCODE -ne 0) {
        Write-Error "cargo build gagal (exit code $LASTEXITCODE). FW_VERSION di main.rs SUDAH terlanjur naik ke $newVersion -- perbaiki error di atas, lalu build ulang manual, atau kembalikan versi kalau mau batal."
        exit 1
    }

    # ---------- 5) Buat firmware.bin ----------
    $elfPath = "target\xtensa-esp32s3-none-elf\release\hive_ota_tb"
    if (-not (Test-Path $elfPath)) {
        Write-Error "Tidak ketemu $elfPath. Cek nama binary di Cargo.toml (mungkin beda dari 'hive_ota_tb')."
        exit 1
    }

    Write-Host "`nMembuat firmware.bin ..."
    espflash save-image --chip esp32s3 $elfPath firmware.bin
    if ($LASTEXITCODE -ne 0) {
        Write-Error "espflash save-image gagal (exit code $LASTEXITCODE)."
        exit 1
    }
}
finally {
    Pop-Location
}

# ---------- 6) Selesai ----------
$firmwarePath = Join-Path $ProjectDir "firmware.bin"
Write-Host "`n=========================================="
Write-Host "SELESAI. Upload versi $newVersion ke ThingsBoard sekarang:"
Write-Host "  File   : $firmwarePath"
Write-Host "  Title  : (samakan dengan FW_TITLE di main.rs)"
Write-Host "  Version: $newVersion"
Write-Host "Lalu assign paket itu ke device DAQ01."
Write-Host "=========================================="
