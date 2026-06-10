# DustiniaDelixia Groceria — Pipeline Data Finance Analyst

Pipeline analisis data end-to-end untuk **Persona 1: Finance Analyst** pada Final Project MCI 2026.

> **Pertanyaan bisnis.** Siapa pelanggan bernilai tinggi (High-Value Customer / HVC) DustiniaDelixia, bagaimana mereka membayar, di mana lokasi geografis mereka, dan berapa pendapatan yang hilang karena perusahaan belum mengoptimalkan opsi pembayaran (cicilan)?

Seluruh stack dijalankan dalam container dan dapat direproduksi: **Airflow** mengorkestrasi ELT yang idempoten, **ClickHouse** menjadi gudang analitis, dan **Metabase** menyajikan dashboard untuk eksekutif. Sebuah **Payment Optimization Revenue Simulator** disertakan sebagai komponen interaktif.

---

## Daftar Isi

1. [Latar belakang & rumusan masalah](#1-latar-belakang--rumusan-masalah)
2. [Arsitektur sistem](#2-arsitektur-sistem)
3. [Dataset](#3-dataset)
4. [Cara menjalankan](#4-cara-menjalankan)
5. [DAG Airflow](#5-dag-airflow)
6. [Pemodelan data ClickHouse](#6-pemodelan-data-clickhouse)
7. [Definisi & metodologi HVC](#7-definisi--metodologi-hvc)
8. [Dashboard Metabase](#8-dashboard-metabase)
9. [Revenue Simulator](#9-nilai-tambah--revenue-simulator)
10. [Temuan utama](#10-temuan-utama)
11. [Kendala teknis & solusinya](#11-kendala-teknis--solusinya)
12. [Struktur repository](#12-struktur-repository)
13. [Tech Stack](#13-tech-stack)

---

## 1. Latar belakang & rumusan masalah

Head of Finance DustiniaDelixia menemukan gap signifikan pada nilai belanja rata-rata: mayoritas pelanggan bertransaksi di kisaran biasa, namun ada segmen tertentu yang bernilai jauh di atas rata-rata. Segmen ini belum pernah dianalisis dari sisi pembayaran. Proyek ini menjawab empat pertanyaan turunan:

- **Siapa** pelanggan bernilai tinggi (HVC) itu?
- **Bagaimana** preferensi metode pembayaran mereka (kartu kredit, cicilan, boleto, voucher)?
- **Di mana** lokasi geografis mereka?
- **Berapa** potensi pendapatan yang hilang akibat opsi pembayaran yang belum optimal?

Hipotesis bisnis yang diuji: ketersediaan opsi cicilan mendorong nilai transaksi lebih tinggi dibanding pembayaran sekali bayar.

---

## 2. Arsitektur sistem

```
CSV Mentah ──> Airflow DAG ───────────────────────────────> ClickHouse ──> Metabase
                |  extract + validasi (per tabel)             |  dustinia_raw.*       dashboard (8 kartu)
                |  load -> dustinia_raw (staging)             |  dustinia_marts.*     + simulator HTML
                |  DQ gate (jumlah baris, orphan record)      |
                |  build marts (dimensi + fakta + agregat)    |
                |  DQ gate (proporsi HVC, nilai negatif)      |
                +- refresh cache Metabase (tambahan)          |
```

Seluruh layanan berjalan dalam satu jaringan Docker Compose: Airflow (webserver + scheduler), ClickHouse, PostgreSQL (metadata Airflow & Metabase), Metabase, dan sebuah web server nginx kecil untuk menyajikan file GeoJSON peta.

Pendekatan **ELT berlapis** (bukan ETL): data mentah dimuat dulu apa adanya ke staging, baru ditransformasi di dalam ClickHouse. Keuntungannya transformasi memanfaatkan kecepatan kolumnar ClickHouse, dan data mentah tetap tersedia untuk audit.

---

## 3. Dataset

Sumber: **Brazilian E-Commerce & Marketing Funnel (Olist)**, disesuaikan untuk studi kasus DustiniaDelixia. Terdiri dari 11 file CSV; enam yang relevan untuk persona Finance:

| Tabel | Baris | Isi & relevansi |
|-------|-------|-----------------|
| `orders` | 99.441 | Status & timestamp tiap pesanan (purchase, approved, delivered) |
| `order_payments` | 103.886 | Metode bayar, jumlah cicilan, nilai — **inti analisis** |
| `order_items` | 112.650 | Harga produk & ongkos kirim per item |
| `customers` | 99.441 | ID unik pelanggan & lokasi (kota, negara bagian, kode pos) |
| `geolocation` | 1.000.161 | Koordinat per kode pos untuk pemetaan geografis |
| `order_reviews` | 99.224 | Skor ulasan (sinyal sekunder) |

Tabel lain (`products`, `sellers`, `category_translation`, `mql`, `closed_deals`) tidak menjadi fokus persona Finance namun tetap dimuat sebagai pelengkap.

**Fokus join utama:** `orders` × `order_payments` × `customers` × `geolocation`.

---

## 4. Cara menjalankan

### Prasyarat
- Docker Desktop (termasuk Docker Compose)
- File `geolocation.csv` (~43MB) disalin manual ke `include/data/` — tidak disertakan di repo karena ukurannya
- Driver ClickHouse untuk Metabase (lihat langkah di bawah)

### Langkah

```bash
# 1. nyalakan seluruh stack
docker compose up -d

# 2. tunggu hingga semua container "healthy" (cek statusnya)
docker compose ps

# 3. akses:
#    Airflow   -> http://localhost:8080   (admin / admin)
#    Metabase  -> http://localhost:3000
#    ClickHouse-> http://localhost:8123
```

### Menjalankan pipeline

```bash
# via UI Airflow: unpause + trigger "dustinia_finance_elt"
# atau via CLI:
docker compose exec airflow-scheduler airflow dags trigger dustinia_finance_elt
```

### Verifikasi data

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM dustinia_marts.dim_customer"
# Harusnya ~95.559

docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM dustinia_marts.dim_customer WHERE is_high_value_customer"
# Harusnya ~9.563
```

DAG bersifat **idempoten** — setiap run membangun ulang marts dari nol, sehingga run berulang tidak pernah menduplikasi data.

---

## 5. DAG Airflow

DAG `dustinia_finance_elt` dijadwalkan harian (`@daily`), maksimum satu run aktif, dengan retry otomatis 2x.

| Task | Tujuan |
|------|--------|
| `create_databases` | Membuat `dustinia_raw` / `dustinia_marts` + DDL staging |
| `extract_validate_*` | Membaca tiap CSV, memastikan dapat di-parse dan tidak kosong (per tabel, paralel) |
| `load_staging_*` | Truncate + bulk-insert ke staging, dengan koersi tipe untuk tanggal & kode pos |
| `dq_gate_staging` | **Gerbang keras**: jumlah baris minimum + pengecekan orphan payment |
| `build_marts` | Mengeksekusi SQL dimensional + agregat |
| `dq_gate_marts` | **Gerbang keras**: proporsi HVC dalam rentang 5–15%, tidak ada nilai order negatif |
| `refresh_metabase_cache` | Tambahan: ping Metabase agar demo selalu menampilkan data terbaru |

### Data Quality Gate

Gerbang data-quality berupa `AirflowFailException` sungguhan — load yang buruk akan **menghentikan pipeline**, bukan diam-diam mengirim angka salah ke dashboard. Ini memastikan integritas data yang disajikan ke pengambil keputusan.

- **Gate staging:** memvalidasi tiap tabel memenuhi jumlah baris minimum, dan tidak ada pembayaran yang tidak punya pesanan terkait (orphan record).
- **Gate marts:** memvalidasi proporsi HVC berada di kisaran wajar (5–15%, sebagai sanity check bahwa logika desil bekerja benar) dan tidak ada nilai pesanan negatif.

### Koersi tipe tanggal

Loader secara eksplisit mendaftarkan kolom bertipe DateTime berdasarkan nama kolom yang tepat (bukan substring), lalu mengubah nilai kosong (NaT) menjadi `None` Python agar diterima ClickHouse `Nullable(DateTime)`. Kolom kode pos dipaksa menjadi string untuk mempertahankan leading zero.

---

## 6. Pemodelan data ClickHouse

Pemodelan berlapis dua skema:

| Layer | Database | Isi |
|-------|----------|-----|
| Staging | `dustinia_raw` | Salinan 1:1 dari CSV sumber (6 tabel) |
| Marts | `dustinia_marts` | Model dimensional + agregat analitis |

### Tabel marts

- **`dim_geolocation`** — satu koordinat representatif per kode pos (geolocation asli punya ~1 juta titik dengan banyak duplikat per prefix; diagregasi dengan `avg` lat/lng).
- **`fct_order_payments`** — tabel fakta, granularitas satu baris per pesanan. Menggulung sekuens pembayaran menjadi metode utama (`argMax` berdasarkan nilai), jumlah cicilan maksimum, penanda penggunaan cicilan, dan pemecahan nilai per metode pembayaran. Dijoin dengan harga item & ongkos kirim.
- **`dim_customer`** — agregasi tingkat pelanggan dengan ukuran RFM (recency, frequency, monetary), rata-rata cicilan, share order bercicilan, dan penetapan segmen nilai + flag HVC.
- **`mart_hvc_payment_profile`** — jawaban utama untuk Head of Finance: profil pembayaran per segmen × metode pembayaran (jumlah order, revenue, AOV, persentase cicilan).
- **`mart_geo_value`** — konsentrasi HVC per negara bagian (penetrasi, revenue, share) untuk peta.
- **`mart_payment_uplift`** — tabel yang menghitung peluang uplift pembayaran (lihat bagian 9).

Engine yang dipakai `MergeTree` dengan `ORDER BY` sesuai kunci akses, ditambah `LowCardinality(String)` untuk kolom kategorikal (status, metode bayar, negara bagian) demi efisiensi.

---

## 7. Definisi & metodologi HVC

**High-Value Customer (HVC)** didefinisikan sebagai pelanggan pada **desil teratas (top 10%)** berdasarkan total belanja seumur hidup (lifetime monetary value). Threshold dihitung secara dinamis dengan `quantileExact(0.90)` atas seluruh basis pelanggan, bukan angka statis — sehingga adaptif terhadap data.

Segmentasi bertingkat:

| Segmen | Definisi |
|--------|----------|
| Top 5% Whale | monetary ≥ persentil 95 |
| High Value | persentil 90 ≤ monetary < persentil 95 |
| Mid Value | persentil 50 ≤ monetary < persentil 90 |
| Standard | monetary < persentil 50 |

Pesanan berstatus `canceled` dikeluarkan dari seluruh analisis agar tidak mengotori nilai belanja.

**Catatan metodologi penting:** hubungan antara penggunaan cicilan dan nilai pesanan yang lebih tinggi bersifat **korelasional, bukan kausal**. Pengguna cicilan mungkin berbeda secara sistematis dari non-pengguna. Karena itu proyeksi uplift diperlakukan sebagai hipotesis berbatas atas yang perlu divalidasi melalui A/B test atau rollout terukur.

---

## 8. Dashboard Metabase

### Setup koneksi

1. Pasang driver ClickHouse: unduh `clickhouse.metabase-driver.jar` dari [rilis resmi](https://github.com/ClickHouse/metabase-clickhouse-driver/releases), letakkan di folder `metabase-plugins/` (di-mount ke `/plugins` container), restart Metabase.
2. Tambah database di Metabase: tipe ClickHouse, host `clickhouse`, port `8123`, database `dustinia_marts`, user `default`, password kosong, SSL mati.

### Peta GeoJSON (Card 5)

Metabase tidak menyediakan peta negara bagian Brazil bawaan. Solusinya: file GeoJSON Brazil (properti `sigla` berisi kode SP/RJ/MG) disajikan via web server nginx dalam jaringan Docker, lalu didaftarkan sebagai Custom Map di Admin → Maps dengan URL `http://geojson/brazil-states.json`. **Penting:** ekstensi harus `.json` (bukan `.geojson`) agar content-type yang dikirim nginx dapat diterima Metabase.

### Delapan kartu dashboard

File `metabase/dashboard_questions.sql` berisi query siap-tempel untuk:

1. **KPI** — total revenue & share HVC (R$6,09 juta, 38,4%)
2. **AOV per segmen** — jurang belanja ~5x antar segmen
3. **Komposisi metode pembayaran HVC** — donut chart
4. **Perilaku cicilan per segmen** — persentase order bercicilan
5. **Konsentrasi geografis HVC** — peta negara bagian Brazil
6. **Slide andalan** — uplift dari optimasi pembayaran
7. **Tren revenue HVC** — garis bulanan HVC vs pelanggan lain
8. **Kota HVC teratas** — tabel terperingkat, dapat difilter

Dashboard dirancang untuk **audiens non-teknis** — setiap kartu menjawab pertanyaan yang benar-benar ditanyakan Head of Finance.

---

## 9. Revenue Simulator

`mart_payment_uplift` mengukur selisih AOV antara order dengan cicilan dan tanpa cicilan untuk tiap segmen, lalu memproyeksikan uplift jika lebih banyak order kartu kredit non-cicilan beralih ke cicilan.

**Logika perhitungan:**
- Populasi addressable = order kartu kredit yang belum memakai cicilan.
- Asumsi konservatif: 30% dari order tersebut beralih ke cicilan dan naik ke AOV cicilan.
- Uplift = jumlah order addressable × 30% × selisih AOV (cicilan − non-cicilan).

Simulator interaktif (`metabase/revenue_simulator.html`) — file HTML berdiri sendiri, cukup dibuka di browser tanpa server — memungkinkan Head of Finance menggeser dua kendali secara langsung saat demo:

- **Tingkat adopsi cicilan** — berapa persen order addressable yang beralih
- **AOV lift capture** — seberapa besar selisih AOV yang teramati benar-benar terealisasi

Angka-angka di simulator diambil langsung dari dataset (jumlah order addressable & selisih AOV per segmen). Ini mengubah temuan statis menjadi business case yang dapat diatur dan dipertahankan saat sesi Q&A.

---

## 10. Temuan utama (dari dataset asli)

| Temuan | Angka |
|--------|-------|
| Porsi pelanggan yang tergolong HVC | ~10% (9.563 dari 95.559) |
| Revenue HVC | R$6.090.850 (38,4% total) |
| AOV HVC vs non-HVC | R$607 vs R$112 (~5,4x) |
| AOV per segmen | Top 5% R$852 · High R$362 · Mid R$172 · Standard R$63 |
| Dominasi metode bayar HVC | credit card 80,99% · boleto 16,45% · lain 2,56% |
| AOV cicilan vs sekali bayar | R$198 vs R$121 (**+64%**) |
| Top negara bagian (revenue HVC) | SP, RJ, MG |
| Top kota (revenue HVC) | São Paulo R$763.894 · Rio R$460.793 · Belo Horizonte R$137.544 |
| Proyeksi uplift tahunan (adopsi 30%) | ~R$84.000 |

Peluang uplift absolut terbesar ada di segmen **Standard & Mid-Value** karena volume order, meski selisih AOV per order mereka lebih kecil dibanding whale.

---

## 11. Kendala teknis & solusinya

Dokumentasi kendala nyata yang dihadapi dan diselesaikan selama pengembangan:

| Kendala | Solusi |
|---------|--------|
| `ModuleNotFoundError: clickhouse_connect` di Airflow | Set via `PIP_ADDITIONAL_REQUIREMENTS` di docker-compose; rebuild container agar terinstal ke environment yang benar |
| Task load gagal: `'str' object has no attribute 'timestamp'` | Bug deteksi kolom tanggal — `order_approved_at` terlewat karena tidak mengandung "date"/"timestamp". Diperbaiki dengan daftar nama kolom eksplisit + konversi NaT ke None |
| Metabase: "Session setup failed" saat connect ClickHouse | Driver ClickHouse belum terpasang; unduh `.jar` dan mount ke `/plugins` |
| Peta Brazil tidak muncul (hanya US & World tersedia) | Pakai Custom Map dengan GeoJSON Brazil yang disajikan via nginx |
| GeoJSON: "returned invalid content-type" | Ganti ekstensi `.geojson` → `.json` agar nginx kirim `application/json` |
| Card 4: kolom cicilan kosong | ClickHouse `avg()` atas campuran nilai & NULL menghasilkan NULL; diperbaiki dengan `avgIf(..., col > 0)` |

---

## 12. Struktur repository

```
dustinia_finance/
├── docker-compose.yml                 # orkestrasi 5 layanan + nginx geojson
├── dags/
│   └── dustinia_finance_elt.py        # DAG Airflow
├── sql/
│   ├── staging/01_create_staging.sql  # tabel landing mentah
│   └── marts/02_build_marts.sql       # model dimensional + agregat
├── metabase/
│   ├── dashboard_questions.sql        # 8 query siap tempel
│   └── revenue_simulator.html         # alat interaktif
├── metabase-plugins/                  # driver ClickHouse (.jar) + GeoJSON peta
├── include/
│   ├── data/                          # CSV sumber (geolocation.csv disalin manual)
│   └── scripts/init_metabase_db.sql   # init DB Metabase di Postgres
└── docs/
    ├── DustiniaDelixia_Finance_Paper.pdf      # paper IEEE (ID)
    └── DustiniaDelixia_Finance_Paper_EN.pdf   # paper IEEE (EN)
```

---

## 13. Tech Stack

- **Orkestrasi:** Apache Airflow 2.9 (LocalExecutor)
- **Gudang data:** ClickHouse 24.8 (MergeTree, kolumnar)
- **BI / Dashboard:** Metabase v0.50 + driver ClickHouse
- **Metadata & app DB:** PostgreSQL 16 (dipakai bersama Airflow & Metabase)
- **Penyajian GeoJSON:** nginx (alpine)
- **Containerisasi:** Docker Compose
- **Sumber data:** Brazilian E-Commerce & Marketing Funnel (Olist), disesuaikan untuk studi kasus DustiniaDelixia Groceria

---

## Komponen wajib — status

| Komponen | Status |
|----------|--------|
| Arsitektur DAG Airflow | OK — ELT idempoten dengan dua data-quality gate |
| ClickHouse | OK — Model berlapis staging → marts |
| Metabase Dashboard | OK — 8 kartu untuk audiens non-teknis |

---

*Disusun oleh Pradhipta Raja Mahendra — Final Project Lab MCI 2026.*
