# DustiniaDelixia Groceria — Pipeline Data Finance Analyst

Pipeline analisis data end-to-end untuk **Persona 1: Finance Analyst** pada Final Project MCI 2026.

> **Pertanyaan bisnis.** Siapa pelanggan bernilai tinggi (High-Value Customer / HVC) DustiniaDelixia, bagaimana mereka membayar, di mana lokasi geografis mereka, dan berapa pendapatan yang hilang karena perusahaan belum mengoptimalkan opsi pembayaran (cicilan)?

Seluruh stack dijalankan dalam container dan dapat direproduksi: **Airflow** mengorkestrasi ELT yang idempoten, **ClickHouse** menjadi gudang analitis, dan **Metabase** menyajikan dashboard untuk eksekutif. Sebuah **Payment Optimization Revenue Simulator** disertakan sebagai komponen nilai tambah (value-add).

---

## 1. Arsitektur

```
CSV Mentah ──> Airflow DAG ───────────────────────────────> ClickHouse ──> Metabase
                |  extract + validasi (per tabel)             |  dustinia_raw.*       dashboard
                |  load -> dustinia_raw (staging)             |  dustinia_marts.*     + simulator
                |  DQ gate (jumlah baris, orphan record)      |
                |  build marts (dimensi + fakta + agregat)    |
                |  DQ gate (proporsi HVC, nilai negatif)      |
                +- refresh cache Metabase (tambahan)          |
```

Pemodelan berlapis:

| Layer | Database | Isi |
|-------|----------|-----|
| Staging | `dustinia_raw` | Salinan 1:1 dari CSV sumber |
| Marts | `dustinia_marts` | `dim_customer`, `fct_order_payments`, `dim_geolocation`, dan empat tabel agregat analitis |

Marts mencakup `mart_hvc_payment_profile`, `mart_geo_value`, dan tabel nilai tambah `mart_payment_uplift`.

---

## 2. Cara Menjalankan

```bash
# 1. letakkan file CSV dataset di ./include/data/
# 2. nyalakan seluruh stack
docker compose up -d

# 3. tunggu health check, lalu akses:
#    Airflow   -> http://localhost:8080   (admin / admin)
#    Metabase  -> http://localhost:3000
#    ClickHouse-> http://localhost:8123
```

Menjalankan pipeline:

```bash
# dari UI Airflow, unpause + trigger "dustinia_finance_elt"
# atau via CLI:
docker compose exec airflow-scheduler airflow dags trigger dustinia_finance_elt
```

DAG berjalan end-to-end dalam beberapa menit dan bersifat **idempoten** — setiap run membangun ulang marts dari nol, sehingga run berulang tidak pernah menduplikasi data.

> **Catatan dataset.** File `geolocation.csv` (~43MB) tidak disertakan dalam repo karena ukurannya. Salin file aslinya ke `include/data/` sebelum menjalankan pipeline.

> **Catatan driver Metabase.** Metabase membutuhkan driver ClickHouse. Unduh `clickhouse.metabase-driver.jar` dari rilis resmi metabase-clickhouse-driver (github.com/ClickHouse/metabase-clickhouse-driver/releases), letakkan di folder `metabase-plugins/`, lalu jalankan ulang container Metabase.

---

## 3. DAG (`dustinia_finance_elt`)

| Task | Tujuan |
|------|--------|
| `create_databases` | Membuat `dustinia_raw` / `dustinia_marts` + DDL staging |
| `extract_validate_*` | Membaca tiap CSV, memastikan dapat di-parse dan tidak kosong |
| `load_staging_*` | Truncate + bulk-insert ke staging (dengan koersi tipe untuk tanggal/zip) |
| `dq_gate_staging` | **Gerbang keras**: jumlah baris minimum + pengecekan orphan payment |
| `build_marts` | Mengeksekusi SQL dimensional + agregat |
| `dq_gate_marts` | **Gerbang keras**: proporsi HVC dalam rentang 5-15%, tidak ada nilai order negatif |
| `refresh_metabase_cache` | Tambahan: ping Metabase agar demo selalu menampilkan data terbaru |

Gerbang data-quality berupa `AirflowFailException` sungguhan — load yang buruk akan menghentikan pipeline alih-alih diam-diam mengirim angka salah ke dashboard.

---

## 4. Dashboard Metabase

Hubungkan Metabase ke ClickHouse (`host=clickhouse, port=8123, db=dustinia_marts`), lalu tempel setiap query dari `metabase/dashboard_questions.sql` ke SQL question baru. Kartu yang dibuat:

1. KPI — total revenue & share HVC
2. AOV per value segment (jurang ~5x)
3. Komposisi metode pembayaran HVC
4. Perilaku cicilan per segmen
5. Konsentrasi geografis HVC (peta negara bagian Brazil via GeoJSON)
6. **Slide andalan** — uplift dari optimasi pembayaran
7. Tren revenue HVC dari waktu ke waktu
8. Kota-kota HVC teratas (dapat difilter)

Dashboard dirancang untuk **audiens non-teknis** — setiap kartu menjawab pertanyaan yang benar-benar ditanyakan Head of Finance.

---

## 5. Payment Optimization Revenue Simulator

`mart_payment_uplift` mengukur selisih pendapatan antara order dengan cicilan dan tanpa cicilan, lalu memproyeksikan uplift jika lebih banyak order kartu kredit beralih ke cicilan. Simulator interaktif pendamping (`metabase/revenue_simulator.html`) memungkinkan Head of Finance menggeser dua kendali secara langsung saat demo:

- **Tingkat adopsi cicilan** — berapa persen order yang dapat digarap beralih ke cicilan
- **AOV lift capture** — seberapa besar selisih AOV yang teramati benar-benar terealisasi

Ini mengubah temuan statis menjadi business case yang dapat diatur dan dipertahankan. File HTML berdiri sendiri (cukup dibuka di browser, tanpa server).

---

## 6. Temuan utama (dari dataset asli)

- HVC (desil teratas berdasarkan total belanja) hanya **~10% pelanggan** namun berbelanja **~5,4x lebih besar per order** (AOV R$607 vs R$112).
- **Kartu kredit mendominasi** pendapatan HVC (~81%); order dengan cicilan memiliki **AOV 64% lebih tinggi** dibanding order sekali bayar (R$198 vs R$121).
- Pendapatan HVC terkonsentrasi secara geografis — **SP, RJ, MG** menyumbang porsi terbesar, namun negara bagian seperti **RS dan RJ menunjukkan penetrasi HVC yang lebih tinggi** (>10%), menandai target ekspansi.
- Model konservatif (adopsi 30% dari selisih AOV yang teramati) memproyeksikan **~R$84 ribu** pendapatan tahunan yang dapat direbut dari optimasi pembayaran, dengan **segmen Standard dan Mid-Value menyimpan peluang absolut terbesar** karena volume order.

> Catatan metodologi: temuan hubungan cicilan dengan AOV bersifat korelasional, bukan kausal. Angka uplift sebaiknya diperlakukan sebagai hipotesis yang perlu divalidasi melalui A/B test atau rollout terukur.

---

## 7. Struktur repository

```
dustinia_finance/
├── docker-compose.yml
├── dags/
│   └── dustinia_finance_elt.py        # DAG Airflow
├── sql/
│   ├── staging/01_create_staging.sql  # tabel landing mentah
│   └── marts/02_build_marts.sql       # model dimensional + agregat
├── metabase/
│   ├── dashboard_questions.sql        # SQL siap tempel untuk dashboard
│   └── revenue_simulator.html         # alat interaktif nilai tambah
├── metabase-plugins/                  # driver ClickHouse (.jar) + GeoJSON peta
├── include/
│   ├── data/                          # CSV sumber
│   └── scripts/init_metabase_db.sql
└── docs/
    └── DustiniaDelixia_Finance_Paper.docx  # paper format IEEE
```

---

## 8. Komponen wajib

| Komponen | Status |
|----------|--------|
| Arsitektur DAG Airflow | OK — ELT idempoten dengan dua data-quality gate |
| ClickHouse | OK — Model berlapis staging -> marts |
| Metabase Dashboard | OK — 8 kartu untuk audiens non-teknis |
| Nilai tambah | OK — Payment Optimization Revenue Simulator interaktif |

---

## 9. Stack teknologi

- **Orkestrasi:** Apache Airflow 2.9
- **Gudang data:** ClickHouse 24.8
- **BI / Dashboard:** Metabase v0.50
- **Metadata & app DB:** PostgreSQL 16
- **Containerisasi:** Docker Compose
- **Sumber data:** Brazilian E-Commerce & Marketing Funnel (Olist), disesuaikan untuk studi kasus DustiniaDelixia Groceria
