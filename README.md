# PRGsheet - Google Sheets Tabanlı İş Süreçleri Yönetim Sistemi

Google Sheets API ve SQL Server entegrasyonu ile çalışan kapsamlı iş süreçleri otomasyon araçları.

## Genel Bakış

Bu proje, Google Sheets'i merkezi veri deposu olarak kullanarak çeşitli iş süreçlerini (risk analizi, kasa yönetimi, irsaliye takibi, stok yönetimi, sevkiyat, vb.) otomatikleştiren Python uygulamalarından oluşur.

### Temel Özellikler

- **Service Account Yetkilendirme**: Google Sheets API ile güvenli, OAuth2 tabanlı erişim
- **Merkezi Konfigürasyon**: Tüm ayarlar PRGsheet'te merkezi olarak yönetilir
- **SQL Server Entegrasyonu**: MSSQL veritabanından veri çekme ve işleme
- **Şifreli Cache**: Hassas ayarlar yerel olarak şifrelenmiş cache'de saklanır
- **SQL Injection Koruması**: Parametreli sorgular ile güvenlik
- **Otomatik Veri Senkronizasyonu**: Veritabanı ve Google Sheets arasında çift yönlü senkronizasyon
- **PyInstaller Desteği**: Standalone .exe dosyaları oluşturma imkanı

---

## Ana Modüller ve İşlevleri

### 1. **BagKodu.py** - Bağ Kodu Analizi
Malzeme bağ kodlarını SQL Server'dan çeker ve Google Sheets'e aktarır.

**Özellikler:**
- SQL Server'dan bağ kodu verilerini çekme
- Otomatik veri temizleme ve dönüştürme
- Google Sheets'e batch upload
- Duplicate kayıt önleme

---

### 2. **Bakiye.py** - Bakiye Analizi
Cari hesap bakiyelerini takip eder ve analiz eder.

**Özellikler:**
- SQL Server'dan bakiye verilerini sorgulama
- Tarih bazlı bakiye hesaplama
- Google Sheets otomasyonu
- Detaylı bakiye raporlama

---

### 3. **BekleyenAPI.py** - Bekleyen Sipariş Sorgulama (Full)
Doğtaş API'sinden bekleyen siparişleri tam tarama ile çeker.

**Özellikler:**
- Doğtaş REST API entegrasyonu
- purchaseInvoiceDate="00000000" filtresi (bekleyen siparişler)
- Dinamik tarih aralığı (RegistrationDateStart ayardan okunur)
- 26 gereksiz sütun otomatik temizleme
- Duplicate elimination (orderId + orderLineId kombinasyonu)
- Mevcut verilerle birleştirme ve güncelleme
- Token yönetimi ve otomatik yenileme
- BagKoduBekleyen sütunu oluşturma
- Batch Google Sheets upload

**Veri Pipeline:**
1. API'den ham veri çekme
2. purchaseInvoiceDate filtresi
3. Duplicate elimination
4. Mevcut verilerle merge
5. Sütun temizleme ve Türkçeleştirme
6. Sheets'e kaydetme

---

### 4. **BekleyenFast.py** - Bekleyen Sipariş Hızlı Güncelleme
BekleyenAPI.py'nin optimize edilmiş versiyonu - sadece son 7 günlük veri.

**Özellikler:**
- Son 7 günlük veri çekme (90% daha hızlı)
- Incremental data loading
- Mevcut verilerle karşılaştırma
- Sadece yeni kayıtları ekleme
- Düşük API kullanımı
- Performans odaklı tasarım

**Avantajları:**
- %90 daha hızlı çalışma (7 gün vs 210+ gün)
- Düşük API quota kullanımı
- Mevcut veri korunur
- Otomatik duplicate kontrolü

---

### 5. **Ciro.py** - Ciro Hesaplama Sistemi
Şube bazında ciro hesaplamaları yapar.

**Özellikler:**
- Ciro.txt dosyasından veri okuma
- Şube bazlı ciro analizi
- Tarih ve dönem bazlı raporlama
- Google Sheets'e otomatik aktarım
- Merkez şube kodu ayarı (PRGsheet'ten)

**Ayarlar:**
- `CIRO_TXT_DOSYA_YOLU`: Ciro.txt dosya yolu
- `MERKEZ_SUBE_KODU`: Merkez şube kodu

---

### 6. **Fiyat_Mikro.py** - Fiyat Karşılaştırma Sistemi
Excel dosyalarından fiyat verilerini okur ve karşılaştırır.

**Özellikler:**
- Çoklu Excel dosyası okuma
- SAP TOPTAN dizini taraması
- Fiyat karşılaştırması ve analiz
- Otomatik raporlama
- Google Sheets entegrasyonu

**Ayarlar:**
- `SAP_TOPTAN_DIR`: SAP TOPTAN Excel dosyaları dizini

---

### 7. **Irsaliye.py** - İrsaliye Takip Sistemi
Fatura - İrsaliye eşleştirmesi ve takibi.

**Özellikler:**
- SQL Server'dan irsaliye verisi
- Excel dosyalarından ek veri
- Fatura - İrsaliye eşleştirme
- Eksik irsaliye tespiti
- 127 gün geriye bakış (ayarlanabilir)
- Detaylı takip raporları

**Ayarlar:**
- `IRSALIYE_DAYS_LOOKBACK`: Kaç gün geriye bakılacak (varsayılan: 127)

---

### 8. **Kasa.py** - Kasa Hareketleri Yönetimi
Kasa giriş/çıkış işlemlerini takip eder.

**Özellikler:**
- SQL Server'dan kasa verileri
- Tarih bazlı filtreleme
- Bakiye hesaplamaları
- Detaylı hareket raporları
- Google Sheets senkronizasyonu

---

### 9. **Montaj.py** - Montaj Raporu Sistemi
Montaj işlemlerini Excel'den okur ve Sheets'e aktarır.

**Özellikler:**
- Excel dosyasından veri okuma
- Montaj raporu analizi
- Otomatik veri temizleme
- Google Sheets upload

**Ayarlar:**
- `MONTAJ_EXCEL_PATH`: Montaj raporu Excel dosya yolu

---

### 10. **OKC.py** - Ödeme Kaydedici Cihaz Fatura Analizi
OKC faturalarını Excel'den yükler ve Sheets'e kaydeder.

**Özellikler:**
- GUI ile Excel dosya seçimi
- Tkinter file dialog
- Tarih bazlı veri filtreleme
- Otomatik duplike kayıt önleme
- Kullanıcı dostu arayüz
- Google Sheets entegrasyonu

---

### 11. **Risk.py** - Risk Analizi Sistemi
Müşteri risk analizi ve takibi.

**Özellikler:**
- SQL Server'dan müşteri verileri
- Risk skorlama algoritması
- Limit ve bakiye karşılaştırması
- Otomatik risk raporları
- Google Sheets güncellemesi

---

### 12. **SanalPos.py** - Sanal POS İşlemleri
Sanal POS işlemlerini SQL ve Excel'den birleştirir.

**Özellikler:**
- SQL Server ve Excel veri birleştirme
- İrsaliye eşleştirme
- Ödeme takibi
- Detaylı işlem raporları

**Ayarlar:**
- `SANALPOS_EXCEL_PATH`: SanalPos Excel dosya yolu

---

### 13. **SAP_Kodu_Olustur.py** - SAP Kodu Üretimi
Stok verilerini SQL'den çeker ve 270'şer satırlık dosyalara böler.

**Özellikler:**
- ID > 8000 olan stok verilerini filtreleme
- 270 satırlık parçalara bölme
- SAP import formatında Excel çıktısı
- Otomatik dosya oluşturma

**Kullanım Amacı:**
SAP sistemine toplu stok aktarımı için uygun formatta dosyalar oluşturur.

---

### 14. **Sevkiyat.py** - Sevkiyat Borç Analizi
Cari, borç, malzeme, plan ve bekleyen siparişleri analiz eder.

**Özellikler:**
- Çoklu SQL sorguları (Cari, Borç, Malzeme, Plan)
- Karmaşık veri birleştirme
- Bekleyen sipariş analizi
- Sevkiyat planlaması
- Detaylı borç raporları
- Google Sheets senkronizasyonu

**Veri Kaynakları:**
- SQL Server (Cari, Borç, Malzeme, Plan)
- Google Sheets (Bekleyen siparişler)

---

### 15. **Siparis.py** - Sipariş Analizi
Tekil sipariş verilerini analiz eder.

**Özellikler:**
- SQL Server'dan sipariş verileri
- Tarih bazlı filtreleme
- Sipariş durum takibi
- Batch processing ile performans
- Google Sheets güncellemesi

---

### 16. **Siparisler.py** - Toplu Sipariş Analizi
Çoklu sipariş verilerini toplu olarak işler.

**Özellikler:**
- Batch sipariş sorgulama
- Toplu veri işleme
- Performans optimizasyonu
- Detaylı sipariş raporları

---

### 17. **SSH.py** - Servis Sipariş Hareketleri
Servis sipariş hareketlerini Excel'den okur.

**Özellikler:**
- Excel dosyasından veri okuma
- Servis sipariş analizi
- Hareket takibi
- Google Sheets entegrasyonu

**Ayarlar:**
- `SSH_EXCEL_PATH`: SSH Excel dosya yolu

---

### 18. **Stok.py** - Stok Yönetim Sistemi
Kapsamlı stok yönetimi ve analizi.

**Özellikler:**
- SQL Server'dan stok verileri
- Stok seviye takibi
- Minimum stok uyarıları
- Detaylı stok raporları
- Google Sheets senkronizasyonu
- Pandas optimizasyonları

---

### 19. **Tamamlanan.py** - Tamamlanan Siparişler
Tamamlanmış sipariş analizleri.

**Özellikler:**
- SQL Server'dan tamamlanan siparişler
- Tarih bazlı raporlama
- Sipariş tamamlanma analizi
- Google Sheets güncellemesi

---

### 20. **central_config.py** - Merkezi Konfigürasyon Yöneticisi
Tüm uygulamalar için merkezi yapılandırma.

**Özellikler:**
- Google Service Account yönetimi
- PRGsheet'ten ayarları çekme
- Şifreli cache sistemi (Fernet)
- SQL Server bağlantı yönetimi
- Spreadsheet ID yönetimi
- Global ve uygulama özel ayarlar

**Saklanan Ayarlar:**
- SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
- Dosya yolları (Excel, txt, vb.)
- API credentials (Doğtaş API)
- Uygulama özel parametreler

**Cache Sistemi:**
- Fernet şifreleme
- `.settings_key` - Şifreleme anahtarı
- `.settings_cache` - Şifrelenmiş ayarlar

---

## Kurulum

### 1. Gereksinimler

```bash
pip install -r requirements.txt
```

**Temel Kütüphaneler:**
- `gspread` - Google Sheets API
- `google-auth` - Google OAuth2
- `pandas` - Veri işleme
- `pyodbc` - SQL Server bağlantısı
- `cryptography` - Cache şifreleme
- `requests` - API çağrıları
- `python-dateutil` - Tarih işlemleri

### 2. Google Service Account Kurulumu

1. Google Cloud Console'da proje oluşturun
2. Google Sheets API'yi etkinleştirin
3. Service Account oluşturun
4. JSON key dosyasını indirin
5. Dosyayı `service_account.json` olarak kaydedin
6. **ASLA GitHub'a yüklemeyin!**

### 3. PRGsheet Yapılandırması

PRGsheet'te `Ayar` sayfası oluşturun ve şu ayarları ekleyin:

| Key | Value | App | Açıklama |
|-----|-------|-----|----------|
| SQL_SERVER | server_adresi | Global | SQL Server adresi |
| SQL_DATABASE | veritabanı_adı | Global | Veritabanı adı |
| SQL_USERNAME | kullanıcı_adı | Global | SQL kullanıcı adı |
| SQL_PASSWORD | şifre | Global | SQL şifresi |
| CIRO_TXT_DOSYA_YOLU | D:/path/Ciro.txt | Ciro | Ciro dosya yolu |
| SAP_TOPTAN_DIR | D:/path/SAP/TOPTAN | Fiyat_Mikro | SAP Excel dizini |
| MONTAJ_EXCEL_PATH | D:/path/Montaj.xlsx | Montaj | Montaj Excel yolu |
| ... | ... | ... | ... |

---

## Kullanım

### Tek Bir Modülü Çalıştırma

```bash
python Risk.py
python Kasa.py
python BekleyenAPI.py
```

### İlk Çalıştırma

1. `service_account.json` dosyasını kök dizine kopyalayın
2. PRGsheet'te ayarları yapılandırın
3. İlgili modülü çalıştırın
4. Ayarlar otomatik olarak şifreli cache'e kaydedilir

### Cache Yönetimi

Cache otomatik olarak yönetilir:
- İlk çalıştırmada PRGsheet'ten ayarlar çekilir
- Ayarlar şifrelenip `.settings_cache` dosyasına kaydedilir
- Sonraki çalıştırmalarda cache'ten okunur
- Cache'i temizlemek için `.settings_cache` dosyasını silin

---

## Güvenlik

### ASLA Paylaşılmaması Gerekenler

- `service_account.json` - Google Service Account credentials
- `.settings_key` - Şifreleme anahtarı
- `.settings_cache` - Şifrelenmiş ayarlar
- `*.json` dosyaları
- PRGsheet ID'si (kodda sabit)

### .gitignore

Tüm hassas dosyalar `.gitignore`'da tanımlıdır:
- `service_account.json`
- `.settings_key`
- `.settings_cache`
- `*.json`
- Build dosyaları
- Cache klasörleri

---

## PyInstaller ile Executable Oluşturma

**NOT:** Build scriptleri bu repository'de bulunmamaktadır (güvenlik).

```bash
pyinstaller --onefile --windowed Risk.py
```

### Build Özellikleri

- Frozen exe desteği (`sys.frozen` kontrolü)
- Dinamik dosya yolu çözümlemesi
- Log klasörü otomatik oluşturma
- Embedded resources desteği

---

## Logging

Her modül kendi log dosyasını oluşturur:

```
logs/
├── risk_analizi.log
├── kasa_analizi.log
├── bekleyen_api.log
├── sevkiyat_analizi.log
└── ...
```

**Log Seviyesi:** ERROR (sadece hatalar kaydedilir)

---

## Proje Yapısı

```
PRGsheet/
├── BagKodu.py              # Bağ kodu analizi
├── Bakiye.py               # Bakiye analizi
├── BekleyenAPI.py          # Bekleyen sipariş (full)
├── BekleyenFast.py         # Bekleyen sipariş (fast)
├── Ciro.py                 # Ciro hesaplama
├── Fiyat_Mikro.py          # Fiyat karşılaştırma
├── Irsaliye.py             # İrsaliye takibi
├── Kasa.py                 # Kasa yönetimi
├── Montaj.py               # Montaj raporu
├── OKC.py                  # OKC fatura analizi
├── Risk.py                 # Risk analizi
├── SanalPos.py             # Sanal POS işlemleri
├── SAP_Kodu_Olustur.py     # SAP kodu üretimi
├── Sevkiyat.py             # Sevkiyat analizi
├── Siparis.py              # Sipariş analizi
├── Siparisler.py           # Toplu sipariş analizi
├── SSH.py                  # Servis sipariş hareketleri
├── Stok.py                 # Stok yönetimi
├── Tamamlanan.py           # Tamamlanan siparişler
├── central_config.py       # Merkezi konfigürasyon
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # Bu dosya
```

---

## API Entegrasyonları

### Doğtaş API (BekleyenAPI.py, BekleyenFast.py)

**Endpoints:**
- Token: `/auth/login`
- Orders: `/orders/search`

**Özellikler:**
- Bearer token authentication
- Tarih bazlı filtreleme
- purchaseInvoiceDate filtresi
- Batch data processing

---

## SQL Server Entegrasyonu

### Bağlantı Yönetimi

- ODBC Driver 17 for SQL Server
- Context manager ile otomatik bağlantı yönetimi
- Connection pooling
- Timeout ayarları
- TrustServerCertificate desteği

### Güvenlik

- Parametreli sorgular (SQL injection koruması)
- Credentials PRGsheet'te saklanır
- Kodda hardcoded şifre YOK

---

## Katkıda Bulunma

Bu proje özel kullanım içindir. Katkıda bulunmak için lütfen iletişime geçin.

---

## Lisans

Özel kullanım - Tüm hakları saklıdır.

---

## İletişim

Sorularınız için lütfen proje yöneticisi ile iletişime geçin.

---

## Sürüm Notları

### v1.0 (Kasım 2024)
- Service Account mimarisine geçiş
- Merkezi konfigürasyon sistemi
- Şifreli cache implementasyonu
- 20 ana modül aktif
- SQL Server entegrasyonu
- Doğtaş API entegrasyonu
- PyInstaller desteği

---

**Not:** Bu README, projenin genel yapısını ve her modülün işlevini açıklar. Detaylı kullanım için her modülün kendi dokümantasyonunu kontrol edin.
