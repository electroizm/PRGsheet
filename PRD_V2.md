# PRD: Doğtaş Bayi Ticari Prim Hesaplama Sistemi (V2)

## 1. Proje Özeti

Bu proje, kullanıcı tarafından girilen manuel aylık hedef verileri ile Doğtaş API'sinden çekilen gerçekleşen sipariş ve fatura verilerini karşılaştırarak, **Doğtaş 2026 Q1 Ticari Politikası**'na uygun prim hakedişlerini hesaplayan bir otomasyon yazılımıdır.

Sistem, `BekleyenAPI.py` içerisindeki mevcut altyapıyı referans alacak ancak sadece bekleyen siparişlerle sınırlı kalmayıp, faturalanmış siparişleri de kapsayacak şekilde genişletilecektir. İptal edilen siparişler hesaplamaya dahil edilmeyecektir.

## 2. Kullanıcı Akışı (User Flow)

1.  **Dönem Seçimi:** Kullanıcı hesaplama yapmak istediği yılı ve çeyreği seçer (Örn: 2026 Q1).
2.  **Hedef Girişi:** Seçilen çeyreğin 3 ayı için ayrı ayrı "Sipariş Hedef Tutarı" manuel olarak girilir.
3.  **Kayıt:** Girilen hedefler Google Sheets üzerinde "Hedef" adlı sayfada saklanır, böylece program yeniden açıldığında tekrar girişe gerek kalmaz.
4.  **Veri Çekme:** Program, belirlenen tarih aralığındaki tüm satış verilerini API'den çeker.
5.  **Hesaplama:**
    - Her ay için Hedef Gerçekleşme Oranı (HGO) hesaplanır ve aylık prim belirlenir.
    - Çeyrek sonunda toplam hacim hedefine bakılarak ek prim hesaplanır.
6.  **Raporlama:** Sonuçlar (Aylık HGO, Kazanılan Primler, Toplam Kazanç) detaylı bir tablo olarak sunulur.

## 3. Fonksiyonel Gereksinimler

### 3.1. Veri Depolama (Storage)

- **Hedef Veritabanı:** Kullanıcının girdiği hedefler Google Sheets üzerindeki **"Hedef"** sayfasında saklanmalıdır.
- **Veri Yapısı:** Sütunlar: `Yıl`, `Çeyrek`, `Ay`, `Hedef Tutar` (Örn: 2026, Q1, 1, 10000000.00)

### 3.2. Kullanıcı Girişi (Input)

- Kullanıcı arayüzü (CLI veya basit GUI) üzerinden yıl ve çeyrek seçimi yapılabilmelidir.
- Negatif sayı veya harf girişini engelleyen validasyonlar bulunmalıdır.
- Google Sheets'ten mevcut hedefler okunup kullanıcıya gösterilmeli ve güncelleme imkanı sunulmalıdır.

### 3.3. API Entegrasyonu ve Veri Eşleştirme (Kritik Güncelleme)

- **Referans:** Doğtaş API bağlantısı için `BekleyenAPI.py` dosyasındaki kimlik doğrulama (Auth) yapısı kullanılacaktır.
- **Veri Kapsamı:**
  - Mevcut modül sadece bekleyenleri (`purchaseInvoiceDate="00000000"`) çekmektedir.
  - **Yeni Kural:** Tarih filtresi kaldırılarak **hem bekleyen hem de faturalanmış** tüm siparişler çekilecektir.
- **Filtreleme:**
  - **İptal Kontrolü:** `orderStatus` veya ilgili alan kontrol edilerek, iptal edilmiş siparişler veri setinden çıkarılacaktır.
- **Veri Alanları:** Hesaplama için aşağıdaki alanlar işlenecektir:
  - `Sipariş Tarihi` (Dönem kontrolü için)
  - `Sipariş Tutarı` (HGO hesaplaması için)
  - `Fatura Tutarı` (Prim hak edişi faturadan hesaplandığı için)
  - `Fatura Tarihi` (Döneme dahil edilip edilmeyeceği kontrolü)

### 3.4. Hesaplama Motoru (Business Logic)

#### A. Aylık Prim Hesaplaması

Formül: `HGO = (Gerçekleşen Sipariş / Hedef) * 100`

| HGO Aralığı   | Prim Oranı |
| :------------ | :--------- |
| %120 ve üzeri | %3.0       |
| %110 - %119   | %2.5       |
| %100 - %109   | %2.0       |
| %90 - %99     | %1.5       |
| %90 altı      | %0.0       |

- **Önemli:** Prim oranı belirlendikten sonra, bu oran o ayki **Net Fatura Tutarı** ile çarpılarak ayın primi bulunur.

#### B. Çeyrek Bazlı Ek Hacim Primi

- **Koşul:** 3 Aylık Toplam HGO $\ge$ %100 olmalıdır.
- **Hesaplama:** Toplam Sipariş Tutarı aşağıdaki barajlara göre değerlendirilir.

| Ciro Barajı  | Ek Prim Oranı |
| :----------- | :------------ |
| 50 Milyon TL | +%1.50        |
| 35 Milyon TL | +%1.25        |
| 20 Milyon TL | +%1.00        |
| 10 Milyon TL | +%0.75        |
| 7 Milyon TL  | +%0.50        |

- Hakedilen oran, çeyrekteki **Toplam Net Fatura Tutarı**'na uygulanır.

## 4. Teknik Mimari (Dosya Yapısı)

- `DogtasPrimHesaplama.py`: Tüm uygulamanın (API bağlantısı, hesaplama mantığı, veri saklama ve kullanıcı arayüzü) tek bir dosyada birleştirilmiş halidir.
  - **Class/Modül Yapısı:**
    - `PrimApiClient`: Veri çekme işlemleri.
    - `PrimCalculator`: Hesaplama mantığı.
    - `StorageManager`: Google Sheets işlemleri.
    - `main`: Uygulama akışı.
- `PRD_V2.md`: Bu doküman.

## 5. Başarı Kriterleri

1.  Program çalıştığında kayıtlı hedefleri hatırlamalıdır.
2.  API'den hem bekleyen hem faturalı verileri eksiksiz getirmelidir.
3.  İptal edilmiş bir sipariş, HGO'yu veya primi etkilememelidir.
4.  Hesaplamalar ticari politika tablosuna birebir uymalıdır.
