# PRG Proje Analizi & Otomasyon Araçları

Bu programlar yani .exe dosyaları, ERP entegrasyonu, veri analizi ve raporlama için tasarlanmış Python betiklerinin bir koleksiyonunu içerir. Araçlar, stok yönetimi, sipariş işleme, finansal raporlama ve servis operasyonları gibi çeşitli iş süreçlerini otomatize etmek için öncelikle SQL Server veritabanları ve Google E-Tablolar (Sheets) ile etkileşime girer.

## Proje Yapısı

Proje, ayarları, API anahtarlarını ve veritabanı kimlik bilgilerini güvenli bir şekilde yönetmek için arka uç olarak Google E-Tabloları (`PRGsheet`) kullanan merkezi bir yapılandırma sistemine (`central_config.py`) dayanır.

### Temel Bileşenler

- **`central_config.py`**: Projenin omurgası. Şunları yönetir:
  - Servis Hesapları (Service Accounts) aracılığıyla güvenli Google E-Tablolar erişimi.
  - Merkezi yapılandırma yönetimi (ayarları bir ana Google E-Tablosundan yükleme).
  - Hassas verilerin şifrelenmiş yerel önbelleğe alınması.
- **`BagKodu.py`**: SQL enjeksiyon koruması ve barkod veri işleme için güvenlik ve yardımcı modül.

### Alanlara Göre Modüller

#### 📦 Stok & Envanter

- **`Stok.py`**: Kapsamlı stok analizi. "Malzeme", 'Hammadde', 'Yarı Mamül' ve 'Mamül' verilerini birleştirerek ana stok raporunu oluşturur.
- **`Fiyat_Mikro.py`**: Fiyat analizi ve karşılaştırma aracı.

#### 🛍️ Satış & Siparişler

- **`Siparisler.py`**: Aktif siparişleri analiz eder, bunları müşteri ve stok detaylarıyla zenginleştirir.
- **`BekleyenAPI.py`**: Bekleyen siparişleri otomatik olarak çekmek ve işlemek için **Doğtaş API** ile entegre olur.
- **`Sevkiyat.py`**: Sevkiyat planlama ve analizi. Müşteri borcunu/riskini hesaplar ve sevkiyat verilerini düzenler.

#### 💰 Finans & Muhasebe

- **`Bakiye.py`**: Müşteri bakiyelerini ve cari hesap durumlarını analiz eder.
- **`Ciro.py`**: Merkez ve şube lokasyonları için verileri ayırarak aylık ciroyu hesaplar.
- **`Risk.py`**: Müşteri risk değerlendirme modülü. Borç ve işlem geçmişine dayalı olarak yüksek riskli müşterileri belirler.
- **`SanalPos.py`**: Excel raporları ve SQL Server verileri arasındaki Sanal POS işlemlerini mutabakatını yapar.
- **`OKC.py`**: OKC (Ödeme Kaydedici Cihaz) fatura verilerini analiz eder.

#### 🏆 Bayi Ticari Prim (HGO)

- **`HGO.py` / `hgo_module.py`**: Doğtaş Bayi Ticari Prim (HGO) Hesaplama Sistemi. Doğtaş API entegrasyonu ile sipariş/fatura verilerini çekerek dönemsel prim hesaplaması yapar. Kademeli prim oranları, aylık hedef takibi, tahmin ve öneri sistemi içerir.

#### 🛠️ Servis & Operasyonlar

- **`Montaj.py`**: Kurulum ve montaj hizmetlerini takip eder.
- **`SSH.py`**: "Servis Sipariş Hareketleri" / Satış sonrası hizmetlerini analiz eder.

## PRG Masaüstü Uygulaması

Proje, tüm modülleri tek bir arayüzde birleştiren **PyQt5** tabanlı modern bir masaüstü uygulaması içerir. Uygulama `PRG/main.py` üzerinden çalışır ve `PRG.exe` olarak tek dosya halinde dağıtılabilir.

### Mimari

- **Modüler Yapı**: Her iş süreci bağımsız bir modül olarak geliştirilmiştir (`*_module.py`).
- **Event Bus**: Modüller arası iletişim için merkezi olay sistemi.
- **Tema Yönetimi**: Dark tema desteği ile modern arayüz.
- **Navigation Bar**: Üst menüden tek tıkla modüller arası geçiş.
- **Global Data Cache**: Google Sheets verilerini bellekte önbelleğe alarak performans optimizasyonu.
- **Şifre Koruması**: Ayarlar ve Virman modülleri Google Sheets üzerindeki `Pass` sayfasından okunan şifrelerle korunur.

### Modüller

| Modül | Dosya | Açıklama |
|-------|-------|----------|
| Stok | `stok_module.py` | Stok analizi ve envanter yönetimi |
| Sevkiyat | `sevkiyat_module.py` | Sevkiyat planlama ve takibi |
| Barkod | `barkod_module.py` | Barkod okuma ve veri işleme |
| Sözleşme | `sozlesme_module.py` | Sözleşme yönetimi |
| ÖKC YazarKasa | `okc_module.py` | Ödeme Kaydedici Cihaz verileri |
| Risk | `risk_module.py` | Müşteri risk değerlendirmesi |
| SSH | `ssh_module.py` | Servis Sipariş Hareketleri |
| Kasa | `kasa_module.py` | Kasa işlemleri ve takibi |
| Virman | `virman_module.py` | Hesaplar arası virman işlemleri |
| Sanal Pos | `sanalpos_module.py` | Sanal POS mutabakatı |
| İrsaliye | `irsaliye_module.py` | İrsaliye oluşturma ve takibi |
| Fiyat | `fiyat_module.py` | Fiyat analizi ve karşılaştırma |
| **HGO** | `hgo_module.py` | **Doğtaş Bayi Ticari Prim Hesaplama** |
| Ayarlar | `ayar_module.py` | Uygulama ayarları (şifre korumalı) |

### EXE Oluşturma

```bash
pyinstaller PRG_onefile.spec
```

Oluşan `PRG.exe` dosyası `dist/` klasöründe bulunur. Tüm bağımlılıklar ve modüller tek dosyada paketlenir.

## Temel Özellikler

- **Güvenli Kimlik Doğrulama**: Hassas kullanıcı kimlik bilgilerinin yerel olarak depolanmasından kaçınmak için Google API'leri için Servis Hesaplarını kullanır.
- **Merkezi Yapılandırma**: Tüm dosya yolları, veritabanı bağlantı dizeleri ve API anahtarları merkezi olarak `PRGsheet` Google E-Tablosunda yönetilir.
- **Veri Entegrasyonu**: MSSQL (ERP verileri) ve Google E-Tablolar (Raporlama/Arayüz) arasında köprü kurar.
- **Otomasyon**: Betikler, kapsamlı kayıt tutma (logging) ile otonom olarak (ör. Görev Zamanlayıcı aracılığıyla) çalışacak şekilde tasarlanmıştır.

## Kurulum & Kullanım

1.  **Bağımlılıklar**:
    ```bash
    pip install pandas pyodbc requests gspread google-auth cryptography openpyxl
    ```
2.  **Yapılandırma**:
    - `service_account.json` dosyasının mevcut olduğundan emin olun (`central_config.py` tarafından yönetilir).
    - `PRGsheet` erişim izinlerini doğrulayın.
3.  **Bir Modülü Çalıştırma**:
    Her betik tipik olarak doğrudan çalıştırılabilir:
    ```bash
    python Stok.py
    ```
    (Not: Belirli komut satırı argümanları veya ortam gereksinimleri için bireysel betikleri kontrol edin).

## Yazar

<div data-spark-custom-html="true">
    <table cellspacing="0" cellpadding="0" border="0" style="border-collapse: collapse; border: none; font-family: sans-serif;">
        <tbody>
            <tr>
                <td style="vertical-align: top; border: none; padding: 0 8px 0 0;">
                     <img src="https://res.spikenow.com/c/?id=576ji8df6q7d6eq2&amp;s=48&amp;m=c&amp;_ts=1xc0n1" width="27" height="27" style="border-radius: 50%; display: block;">
                </td>
                <td style="vertical-align: top; border: none; padding: 0;">
                    <div style="line-height: 1.2;"><a href="https://twitter.com/Guneslsmail" style="text-decoration: none !important; color: #0084ff !important; font-size: 13px; font-weight: bold;">İsmail Güneş</a></div>
                    <div style="line-height: 1.2; margin-top: 2px;"><a href="https://www.instagram.com/dogtasbatman/" style="text-decoration: none !important; color: #0084ff !important; font-size: 12px; font-weight: bold;">Güneşler Elektronik<br>Mühendislik Mobilya</a></div>
                </td>
            </tr>
        </tbody>
    </table>
</div>

**Proje Başlangıç Tarihi:** 15.11.2024

---

# Proje Kod Dosyaları Detaylı Dokümantasyonu

Bu belge, projedeki her bir Python (`.py`) dosyasının amacı, çalışma mantığı ve teknik detaylarını içermektedir.

## 1. Stok Yönetimi ve Ürün Analizi

### `Stok.py`

**Ne İşe Yarar?**
Projenin en kapsamlı stok analiz modülüdür. SQL Server'dan (MIKRO/ERP) ve Google Sheets'ten çeşitli veri setlerini çeker ve birleştirerek ana stok raporunu oluşturur.
**Detaylı İşleyiş:**

- SQL Server'dan `STOKLAR`, `MALZEME` gibi tabloları sorgular.
- Her ürün için güncel stok miktarlarını, bekleyen siparişleri ve depodaki miktarları hesaplar.
- "Bekleyen Siparişler" bilgisini malzemelerle eşleştirir.
- Oluşan devasa veri setini işleyip temizler ve Google Sheets üzerindeki ilgili stok raporu sayfasına yazar.

### `Fiyat_Mikro.py`

**Ne İşe Yarar?**
Fiyat analizi ve fiyat karşılaştırması yapmak için kullanılır.
**Detaylı İşleyiş:**

- Belirli dizinlerdeki CSV formatındaki fiyat listelerini tarar.
- `SAP Kodu` üzerinden ürünleri eşleştirir.
- Farklı kaynaklardan gelen fiyatları (Toptan vb.) yan yana getirerek `Fiyat_Mikro.xlsx` dosyasını oluşturur ve bunu Google Sheets'e yükler.
- Service Account kullanarak sessiz modda çalışır.

### `BagKodu.py`

**Ne İşe Yarar?**
Barkod ve bağ kodu verilerinin güvenli bir şekilde işlenmesini sağlar.
**Detaylı İşleyiş:**

- SQL enjeksiyon saldırılarına karşı korumalı parametreli sorgular içerir.
- Barkod verilerini doğrular ve veritabanına güvenli kayıt/güncelleme işlemleri yapar.
- Genellikle diğer modüller tarafından yardımcı bir araç olarak kullanılır.

## 2. Satış, Sipariş ve Sevkiyat

### `Siparisler.py`

**Ne İşe Yarar?**
Aktif müşteri siparişlerini takip eder ve analiz eder.
**Detaylı İşleyiş:**

- SQL Server'dan açık siparişleri (henüz teslim edilmemiş) çeker.
- Bu siparişleri müşteri bilgileri ve güncel stok durumu ile zenginleştirir (Stokta var mı, üretilmesi mi gerekiyor?).
- Renklendirme ve formatlama kuralları uygulayarak Google Sheets'e "Sipariş Listesi" olarak aktarır.

### `BekleyenAPI.py`

**Ne İşe Yarar?**
Doğtaş API'si ile entegre çalışarak dış sistemdeki siparişleri otomatik içeri alır.
**Detaylı İşleyiş:**

- API token yönetimi (auth) yapar.
- Belirli tarih aralıkları için "Bekleyen Siparişleri" JSON formatında çeker.
- Gelen veriyi yerel veritabanı formatına uygun hale getirir ("Mapping").
- Mükerrer kayıtları engeller ve yeni siparişleri sisteme ekler.

### `Sevkiyat.py`

**Ne İşe Yarar?**
Sevkiyat planlaması ve müşteri risk analizi için kritik bir modüldür.
**Detaylı İşleyiş:**

- Müşterilerin güncel bakiyelerini ve açık hesap risklerini hesaplar.
- Siparişlerin sevk edilebilir durumda olup olmadığını kontrol eder.
- "Hangi müşteriye, hangi ürün, ne zaman gönderilebilir?" sorusuna cevap verir.
- Verileri `Sevkiyat` adlı Google Sheet sayfasına işler.

## 3. Finansal Modüller

### `Bakiye.py`

**Ne İşe Yarar?**
Müşteri cari hesap bakiyelerini listeler.
**Detaylı İşleyiş:**

- SQL Server'dan cari hareketleri sorgular.
- Borç/Alacak bakiyesini hesaplar.
- Bakiyeleri düzenli bir formatta raporlar.

### `Ciro.py`

**Ne İşe Yarar?**
Aylık satış cirosunu hesaplar ve raporlar.
**Detaylı İşleyiş:**

- Satış verilerini, iadeleri ve iskontoları dikkate alarak net ciroyu bulur.
- Merkez ve Şube satışlarını ayrı ayrı kategorize edebilir.
- Sonuçları tarih bazlı olarak saklar.

### `Risk.py`

**Ne İşe Yarar?**
Müşterilerin finansal risk durumunu (Riskli, Takipte, Güvenli vb.) analiz eder.
**Detaylı İşleyiş:**

- Müşterinin toplam borcunu, açık çek/senetlerini ve ödeme alışkanlıklarını analiz eder.
- Belirlenen limitlerin üzerindeki riskleri "Kırmızı Liste" olarak işaretler.
- Satış ekibinin riskli müşteriye mal çıkışı yapmasını engellemek için uyarı mekanizması sağlar.

### `SanalPos.py`

**Ne İşe Yarar?**
Sanal POS üzerinden geçen tahsilatların muhasebeleşmesini kontrol eder.
**Detaylı İşleyiş:**

- Bankadan gelen Excel ekstresi ile SQL Server'daki tahsilat kayıtlarını karşılaştırır.
- Eşleşmeyen veya tutarsız kayıtları raporlar.

### `OKC.py`

**Ne İşe Yarar?**
Ödeme Kaydedici Cihaz (Yazar Kasa POS) verilerini analiz eder.
**Detaylı İşleyiş:**

- Resmi muhasebe kayıtları ile fiili satışları karşılaştırır.

## 4. Bayi Ticari Prim (HGO)

### `HGO.py` / `hgo_module.py`

**Ne İşe Yarar?**
Doğtaş bayileri için ticari prim (HGO - Hedef Gerçekleştirme Oranı) hesaplama sistemidir. Dönemsel sipariş ve fatura verilerine göre bayi prim hak edişini hesaplar.

**Detaylı İşleyiş:**

- **Doğtaş API Entegrasyonu:** `PrimApiClient` sınıfı ile Doğtaş API'sine bağlanır. Sipariş (`OrderTotal`) ve fatura (`InvoiceTotal`) verilerini dönemsel olarak çeker.
- **Dönem Yönetimi:** Her dönem 3 aylık periyotlardan oluşur (Ocak-Mart, Nisan-Haziran, Temmuz-Eylül, Ekim-Aralık). Kullanıcı yıl ve dönem seçimi yapabilir.
- **Aylık Hedefler:** Google Sheets'teki `PRGsheet/Ayar` sayfasından aylık hedef tutarları okunur. Her ay için ayrı hedef belirlenebilir.
- **HGO Hesaplama:** `PrimCalculator` sınıfı ile gerçekleşen sipariş tutarı hedefe bölünerek HGO oranı (%) hesaplanır.
- **Kademeli Prim Oranları:**
  - %0-79 HGO → %0 prim
  - %80-89 HGO → %1 prim
  - %90-99 HGO → %2 prim
  - %100-109 HGO → %3 prim
  - %110-119 HGO → %4 prim
  - %120+ HGO → %5 prim
- **Tahmin ve Öneri:** Mevcut sipariş trendine göre dönem sonu tahmini yapar. Bir üst prim kademesine ulaşmak için gereken ek sipariş tutarını hesaplar.
- **Veri Önbelleği:** `_StorageManager` ile hesaplama sonuçları JSON formatında yerel olarak saklanır. Çevrimdışı erişim ve hızlı yükleme sağlar.
- **Arka Plan İşleme:** `_DataFetchWorker` (QThread) ile API çağrıları arka planda yapılır, arayüz donmaz.

## 5. Operasyon ve Servis (devam)

### `Montaj.py`

**Ne İşe Yarar?**
Satılan ürünlerin kurulum ve montaj süreçlerini takip eder.
**Detaylı İşleyiş:**

- Montaj ekiplerinin iş emirlerini listeler.
- Tamamlanan montajları ve müşteri memnuniyet durumunu raporlar.
- Servis Bakım ID ve Sözleşme Numarası üzerinden takibini yapar.

### `SSH.py` (Servis Sipariş Hareketleri)

**Ne İşe Yarar?**
Satış sonrası hizmetler (SSH), yedek parça ve servis taleplerini yönetir.
**Detaylı İşleyiş:**

- Müşteri şikayet veya servis taleplerini takip eder.
- Yedek parça siparişi gerekiyorsa bunların tedarik durumunu izler.
- Excel'den okuduğu servis verilerini Google Sheets üzerinde merkezi bir tabloyla senkronize eder.

## 6. Altyapı ve Yapılandırma

### `central_config.py`

**Ne İşe Yarar?**
**BU DOSYA PROJENİN BEYNİDİR.** Tüm ayarların merkezi olarak yönetilmesini sağlar.
**Detaylı İşleyiş:**

- **Service Account Yönetimi:** Google API'lerine güvenli erişim için kimlik doğrulama işlemlerini otomatik yapar.
- **Merkezi Ayarlar:** Veritabanı şifreleri, dosya yolları gibi ayarları kodun içinde değil, `PRGsheet` adlı Google E-Tablosunda saklar ve oradan okur.
- **Önbellekleme (Caching):** Performans için ayarları şifreli bir şekilde yerel diskte önbelleğe alır.
- Hemen hemen tüm diğer `.py` dosyaları bu dosyayı `import` ederek çalışır.
