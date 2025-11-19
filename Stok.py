"""
STOK YÖNETİM SİSTEMİ - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli stok yönetim otomasyonu

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Server entegrasyonu
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pandas as pd
import pyodbc
import os
import numpy as np
import sys
import logging
from pathlib import Path

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

# Pandas FutureWarning'i önlemek için ayar
pd.set_option('future.no_silent_downcasting', True)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# PyInstaller ile freeze edildiginde dosya yollarini duzelt
if getattr(sys, 'frozen', False):
    base_dir = Path(sys.executable).parent
else:
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'stok_yonetimi.log'

logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler()  # Sadece konsola yazdır
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Service Account ve Merkezi Config
# ============================================================================

class StokConfig:
    """Configuration management - Service Account kullanır"""

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()

            # PRGsheet'ten ayarları yükle
            self.settings = self.config_manager.get_settings()

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            raise

    @property
    def spreadsheet_name(self) -> str:
        """Hedef spreadsheet adı"""
        return 'PRGsheet'

    @property
    def connection_string(self) -> str:
        """Get database connection string from PRGsheet"""
        required_settings = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
        missing = [key for key in required_settings if not self.settings.get(key)]

        if missing:
            raise ValueError(
                f"PRGsheet → Ayar sayfasında eksik SQL ayarları: {', '.join(missing)}\n"
                f"Gerekli: SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD"
            )

        return (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.settings["SQL_SERVER"]};'
            f'DATABASE={self.settings["SQL_DATABASE"]};'
            f'UID={self.settings["SQL_USERNAME"]};'
            f'PWD={self.settings["SQL_PASSWORD"]};'
            f'TrustServerCertificate=yes;'
            f'Connection Timeout=30;'
            f'Command Timeout=60'
        )

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsYoneticisi:
    """
    Google Sheets API Entegrasyonu ve Veri Yönetimi Sınıfı - Service Account

    Bu sınıf, stok verilerinin Google Sheets'e otomatik aktarımını yönetir.

    Temel İşlevler:
    - Service Account ile Google Sheets authentication
    - Worksheet oluşturma/güncelleme/temizleme operations
    - DataFrame to Sheets format conversion
    - Batch data upload optimization
    - Error handling ve logging

    Risk.py'deki GoogleSheetsManager'dan optimize edilerek uyarlandı
    """

    def __init__(self, config: StokConfig):
        """
        Google Sheets yöneticisini başlatma ve temel konfigürasyon

        İşlem Adımları:
        1. Config'den Service Account client al
        2. Spreadsheet adını belirleme (PRGsheet - sabit)
        3. Google Sheets client authentication
        """
        self.config = config
        self.spreadsheet_name = config.spreadsheet_name

        # Service Account ile yetkilendirilmiş client'i al
        self.gc = config.config_manager.gc

    def sayfa_guncelle(self, sayfa_adi: str, data: pd.DataFrame) -> bool:
        """
        Belirtilen sayfayı DataFrame ile günceller

        Args:
            sayfa_adi: Güncellenecek sayfa adı
            data: Yazılacak DataFrame

        Returns:
            bool: İşlem başarı durumu
        """
        try:
            # Güvenlik kontrolleri
            if data is None:
                logger.error(f"'{sayfa_adi}' için None veri alındı")
                return False

            if not isinstance(data, pd.DataFrame):
                logger.error(f"'{sayfa_adi}' için geçersiz veri tipi: {type(data)}")
                return False

            spreadsheet = self.gc.open(self.spreadsheet_name)

            # Worksheet'i güvenli oluştur/al
            try:
                worksheet = spreadsheet.worksheet(sayfa_adi)
                worksheet.clear()  # Mevcut veriyi temizle
            except:
                # Yeni worksheet oluştur - daha büyük boyutlarla
                worksheet = spreadsheet.add_worksheet(title=sayfa_adi, rows=5000, cols=30)

            if not data.empty:
                # Veri boyutu kontrolü
                max_rows = 4900  # Google Sheets limit'ine yakın ama güvenli
                if len(data) > max_rows:
                    logger.warning(f"Veri çok büyük ({len(data)} satır), ilk {max_rows} satır alınacak")
                    data = data.head(max_rows)

                # Batch güncelleme için veriyi hazırla
                values = [data.columns.values.tolist()] + data.values.tolist()

                # Güvenli güncelleme
                worksheet.update(range_name='A1', values=values, value_input_option='RAW')
                return True
            else:
                logger.warning(f"'{sayfa_adi}' sayfası için boş veri")
                return False

        except Exception as e:
            logger.error(f"'{sayfa_adi}' sayfası güncelleme hatası: {e}")
            return False

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def baglanti_bilgilerini_al(config: StokConfig):
    """
    SQL Server ERP Sistemi Bağlantı Yönetimi - Service Account Versiyonu

    PRGsheet'ten güvenli şekilde veritabanı bağlantı bilgilerini alır
    ve ODBC connection oluşturur. Optimize edilmiş hata yönetimi ile.

    Args:
        config: StokConfig instance (Service Account ile yüklenmiş)

    Returns:
        pyodbc.Connection: Yetkilendirilmiş veritabanı bağlantısı veya None (hata durumu)
    """
    connection = None
    try:
        connection_string = config.connection_string
        connection = pyodbc.connect(connection_string)
        return connection

    except pyodbc.Error as e:
        logger.error(f"SQL Server bağlantı hatası: {e}")
        return None
    except Exception as e:
        logger.error(f"Beklenmeyen bağlantı hatası: {e}")
        return None

# ============================================================================
# SQL DATA EXTRACTION FUNCTIONS
# ============================================================================

def malzeme_listesini_al(config: StokConfig):
    """
    [ADIM 1.2] ERP Sisteminden Master Malzeme Listesi Çekimi

    STOKLAR_CHOOSE_3A tablosundan tüm malzeme bilgilerini çeker.
    Bu tablo, ERP sistemindeki ana stok master data'sini içerir.

    SQL Sorgu Detayları:
    - Tablo: STOKLAR_CHOOSE_3A (ana stok master)
    - Sıralama: msg_S_0870 ASC (malzeme adı alfabetik)
    - Tüm sütunlar dahil (*)

    Returns:
        pd.DataFrame: Master malzeme bilgileri (ad, kod, depo miktarları, vb.)

    Veri İçeriği:
    - Malzeme kodları ve adları
    - DEPO, EXC, SUBE stok miktarları
    - SPEC bilgileri ve diğer master data alanları
    """
    baglanti = baglanti_bilgilerini_al(config)
    if not baglanti:
        return pd.DataFrame()

    try:
        cursor = baglanti.cursor()

        # Malzeme listesi SQL sorgusu
        sql_sorgusu = """
        SELECT * FROM STOKLAR_CHOOSE_3A
        ORDER BY [msg_S_0870] ASC
        """

        cursor.execute(sql_sorgusu)
        satirlar = cursor.fetchall()

        # DataFrame oluştur
        sutun_isimleri = [sutun[0] for sutun in cursor.description]
        df = pd.DataFrame.from_records(satirlar, columns=sutun_isimleri)


        return df

    except Exception as e:
        logger.error(f"Malzeme listesi alınamadı: {e}")
        return pd.DataFrame()
    finally:
        baglanti.close()

def cari_sevkiyat_borcu_al(config: StokConfig):
    """
    [ADIM 1.1] ERP Sisteminden Sevkiyat Borç Analizi Çekimi

    Stored Procedure (sp_SiparisOperasyonlari) çalıştırarak
    cari hesapların sevkiyat borcu durumunu analiz eder.

    Stored Procedure Parametreleri:
    - Tarih aralığı: 2023-2077 (geniş aralık)
    - Tip: Sevkiyat borcu raporu (tip=2)
    - Diğer filtreler: Aktif durumda

    Returns:
        pd.DataFrame: Sevkiyat borç detay raporu

    Veri İçeriği:
    - Cari bilgileri (ad, kod)
    - Malzeme bilgileri (ad, kod, SPEC)
    - Kalan sipariş miktarları
    - Depo ve sorumluk merkezi bilgileri
    - Tarih ve sipariş numarası detayları

    Özel İşlem: SUBE ve EXC depo tipleri için 'Kalan Siparis' sıfırlanır
    """
    baglanti = baglanti_bilgilerini_al(config)
    if not baglanti:
        return pd.DataFrame()

    try:
        cursor = baglanti.cursor()

        # Sevkiyat borcu SQL sorgusu
        sql_sorgusu = """
        SET NOCOUNT ON;
        EXEC dbo.sp_SiparisOperasyonlari 0, '20230101', '20770717', 0, 0, 2, 1, 0, 0, N'', 1, N'', 0, 0, 0, 1
        """

        cursor.execute(sql_sorgusu)
        satirlar = cursor.fetchall()

        # DataFrame oluştur
        sutun_isimleri = [sutun[0] for sutun in cursor.description]
        df = pd.DataFrame.from_records(satirlar, columns=sutun_isimleri)

        # Gerekli sütunları seç ve yeniden adlandır
        df = df[['msg_S_0463', '#msg_S_0469', '#msg_S_0119',
                 '#msg_S_1130', '#msg_S_0260', 'msg_S_0159',
                 'msg_S_0201', 'msg_S_0200',
                 'msg_S_0157', 'msg_S_0241', '#msg_S_0005',
                 'msg_S_0070', 'msg_S_0078']].rename(columns={
            "msg_S_0241": "Tarih",
            "msg_S_0201": "Cari Adi",
            "msg_S_0070": "Malzeme Adı",
            "#msg_S_0005": "SPEC",
            "msg_S_0463": "Kalan Siparis",
            "msg_S_0159": "DEPO",
            "#msg_S_0469": "Toplam Stok",
            "#msg_S_1130": "Satıcı Adi",
            "#msg_S_0119": "Sorumluk Merkezi",
            "msg_S_0200": "Cari Kodu",
            "msg_S_0157": "Sipariş No",
            "msg_S_0078": "Malzeme Kodu",
            "#msg_S_0260": "Açıklama"
        })

        # Sütun sırasını düzenle
        df = df.reindex(columns=[
            "Tarih", "Cari Adi", "Malzeme Adı", "SPEC", "Kalan Siparis",
            "DEPO", "Toplam Stok", "Satıcı Adi", "Sorumluk Merkezi",
            "Cari Kodu", "Sipariş No", "Malzeme Kodu", "Açıklama"
        ])

        # Kalan sipariş verilerini güncelle (SUBE ve EXC için 0 yap)
        df["Kalan Siparis"] = df.apply(
            lambda row: 0 if row["DEPO"] in ["SUBE", "EXC"] else row["Kalan Siparis"],
            axis=1
        )

        return df

    except Exception as e:
        logger.error(f"Sevkiyat borcu alınamadı: {e}")
        return pd.DataFrame()
    finally:
        baglanti.close()

def barkod_bilgilerini_al(config: StokConfig):
    """
    [ADIM 1.3] ERP Sisteminden Barkod ve Bağ Kodu Master Data Çekimi

    BARKOD_TANIMLARI tablosunu STOKLAR tablosu ile LEFT JOIN yaparak
    barkod-malzeme ilişki matrisini oluşturur.

    SQL Join Detayları:
    - Ana tablo: BARKOD_TANIMLARI (WITH NOLOCK - performans)
    - Bağlantı: STOKLAR tablosu (sto_kod = bar_stokkodu)
    - Filtre: Aktif stoklar (sto_pasif_fl IS NULL OR sto_pasif_fl=0)
    - Sıralama: bar_RECno DESC (en yeni kayıtlar önce)

    Returns:
        pd.DataFrame: Barkod-malzeme eşleştirme tablosu

    Veri İçeriği:
    - barkodKayit: Kayıt ID'si
    - bagKodum: Bağ kodu (sipariş-malzeme eşleştirmesi için kritik)
    - malzemeKodu: ERP malzeme kodu
    - malzemeAdi: Malzeme tanımı

    Kullanım Amacı: Bekleyen siparişlerin malzeme kodlarıyla eşleştirilmesi
    """
    baglanti = baglanti_bilgilerini_al(config)
    if not baglanti:
        return pd.DataFrame()

    try:
        cursor = baglanti.cursor()

        # Barkod bilgileri SQL sorgusu
        sql_sorgusu = """
        SELECT TOP 100 PERCENT
            bar_RECno AS [barkodKayit],
            bar_serino_veya_bagkodu AS [bagKodum],
            bar_stokkodu AS [malzemeKodu],
            sto_isim AS [malzemeAdi]
        FROM dbo.BARKOD_TANIMLARI WITH (NOLOCK)
        LEFT OUTER JOIN dbo.STOKLAR ON sto_kod = bar_stokkodu
        WHERE sto_pasif_fl IS NULL OR sto_pasif_fl=0
        ORDER BY bar_RECno DESC
        """

        cursor.execute(sql_sorgusu)
        satirlar = cursor.fetchall()

        # DataFrame oluştur
        sutun_isimleri = [sutun[0] for sutun in cursor.description]
        df = pd.DataFrame.from_records(satirlar, columns=sutun_isimleri)

        return df

    except Exception as e:
        logger.error(f"Barkod bilgileri alınamadı: {e}")
        return pd.DataFrame()
    finally:
        baglanti.close()

# ============================================================================
# GOOGLE SHEETS DATA FUNCTIONS
# ============================================================================

def ayar_verilerini_al(sheets_yoneticisi: GoogleSheetsYoneticisi):
    """
    PRGsheets/Ayar sayfasından ayar verilerini çeker

    Args:
        sheets_yoneticisi: GoogleSheetsYoneticisi instance

    Returns:
        dict: Ayar verileri {'KDV': value, 'Ön Ödeme İskonto': value}
    """
    try:
        # Google Sheets'den Ayar sayfasını oku
        spreadsheet = sheets_yoneticisi.gc.open(sheets_yoneticisi.spreadsheet_name)
        worksheet = spreadsheet.worksheet('Ayar')

        # Veriyi liste olarak al
        data = worksheet.get_all_values()

        ayar_dict = {}

        # Verileri satır satır oku ve ayar değerlerini bul
        for row in data:
            if len(row) >= 2:
                key = row[0].strip()
                value = row[1].strip()

                if key == 'KDV':
                    try:
                        # Virgülü noktaya çevir ve float'a dönüştür
                        value_clean = value.replace(',', '.')
                        ayar_dict['KDV'] = float(value_clean)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"KDV değeri okunamadı ({value}): {e}")
                        ayar_dict['KDV'] = 1.10  # Varsayılan değer

                elif key == 'Ön Ödeme İskonto':
                    try:
                        # Virgülü noktaya çevir ve float'a dönüştür
                        value_clean = value.replace(',', '.')
                        ayar_dict['Ön Ödeme İskonto'] = float(value_clean)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Ön Ödeme İskonto değeri okunamadı ({value}): {e}")
                        ayar_dict['Ön Ödeme İskonto'] = 0.90  # Varsayılan değer

        # Eğer değerler bulunamadıysa varsayılan değerleri ata
        if 'KDV' not in ayar_dict:
            ayar_dict['KDV'] = 1.10
        if 'Ön Ödeme İskonto' not in ayar_dict:
            ayar_dict['Ön Ödeme İskonto'] = 0.90

        return ayar_dict

    except Exception as e:
        logger.warning(f"Ayar sayfası okunamadı: {e}")
        # Varsayılan değerler döndür
        return {'KDV': 1.10, 'Ön Ödeme İskonto': 0.90}

def fiyat_verilerini_al(sheets_yoneticisi: GoogleSheetsYoneticisi):
    """
    PRGsheets/Fiyat sayfasından fiyat verilerini çeker

    Args:
        sheets_yoneticisi: GoogleSheetsYoneticisi instance

    Returns:
        pd.DataFrame: Fiyat verileri (SAP_KODU, TOPTAN, PERAKENDE, LISTE sütunları ile)
    """
    try:
        # Google Sheets'den Fiyat sayfasını oku
        spreadsheet = sheets_yoneticisi.gc.open(sheets_yoneticisi.spreadsheet_name)
        worksheet = spreadsheet.worksheet('Fiyat')

        # Veriyi liste olarak al
        data = worksheet.get_all_values()

        # Başlıkları ayır ve DataFrame oluştur
        if len(data) > 0:
            headers = data[0]
            rows = data[1:] if len(data) > 1 else []
            fiyat_df = pd.DataFrame(rows, columns=headers)

            # Gerekli sütunların varlığını kontrol et
            required_columns = ['SAP Kodu', 'TOPTAN', 'PERAKENDE', 'LISTE']
            if all(col in fiyat_df.columns for col in required_columns):
                # Veri tipi dönüşümü
                fiyat_df['SAP Kodu'] = fiyat_df['SAP Kodu'].astype(str)
                for col in ['TOPTAN', 'PERAKENDE', 'LISTE']:
                    fiyat_df[col] = pd.to_numeric(fiyat_df[col], errors='coerce').fillna(0)

                return fiyat_df
            else:
                missing_cols = [col for col in required_columns if col not in fiyat_df.columns]
                logger.warning(f"Fiyat sayfasında gerekli sütunlar bulunamadı: {missing_cols}")
                return pd.DataFrame(columns=required_columns)
        else:
            logger.warning("Fiyat sayfası boş")
            return pd.DataFrame(columns=['SAP Kodu', 'TOPTAN', 'PERAKENDE', 'LISTE'])

    except Exception as e:
        logger.warning(f"Fiyat sayfası okunamadı: {e}")
        return pd.DataFrame(columns=['SAP Kodu', 'TOPTAN', 'PERAKENDE', 'LISTE'])

def plan_verilerini_al(sheets_yoneticisi: GoogleSheetsYoneticisi):
    """
    PRGsheets/Plan sayfasından plan verilerini çeker

    Args:
        sheets_yoneticisi: GoogleSheetsYoneticisi instance

    Returns:
        pd.DataFrame: Plan verileri (Malzeme Kodu ve Adet sütunları ile)
    """
    try:
        # Google Sheets'den Plan sayfasını oku
        spreadsheet = sheets_yoneticisi.gc.open(sheets_yoneticisi.spreadsheet_name)
        worksheet = spreadsheet.worksheet('Plan')

        # Veriyi liste olarak al
        data = worksheet.get_all_values()

        # Başlıkları ayır ve DataFrame oluştur
        if len(data) > 0:
            headers = data[0]
            rows = data[1:] if len(data) > 1 else []
            plan_df = pd.DataFrame(rows, columns=headers)

            # Malzeme Kodu ve Adet sütunları için veri tipi dönüşümü
            if 'Malzeme Kodu' in plan_df.columns and 'Adet' in plan_df.columns:
                plan_df['Malzeme Kodu'] = plan_df['Malzeme Kodu'].astype(str)
                plan_df['Adet'] = pd.to_numeric(plan_df['Adet'], errors='coerce').fillna(0).astype(int)

                # Malzeme Kodu'na göre gruplandır ve Adet'i topla
                plan_df = plan_df.groupby('Malzeme Kodu', as_index=False).agg({'Adet': 'sum'})

                return plan_df
            else:
                logger.warning("Plan sayfasında 'Malzeme Kodu' veya 'Adet' sütunu bulunamadı")
                return pd.DataFrame(columns=['Malzeme Kodu', 'Adet'])
        else:
            logger.warning("Plan sayfası boş")
            return pd.DataFrame(columns=['Malzeme Kodu', 'Adet'])

    except Exception as e:
        logger.warning(f"Plan sayfası okunamadı: {e}")
        return pd.DataFrame(columns=['Malzeme Kodu', 'Adet'])

def bekleyen_siparisleri_isle_dataframe(sheets_yoneticisi: GoogleSheetsYoneticisi, bagKodu_df):
    try:
        # Google Sheets'den veri çek
        spreadsheet = sheets_yoneticisi.gc.open(sheets_yoneticisi.spreadsheet_name)
        worksheet = spreadsheet.worksheet('Bekleyen')

        # Veriyi liste olarak al
        data = worksheet.get_all_values()

        # Başlıkları ayır ve DataFrame oluştur
        if len(data) > 0:
            headers = data[0]
            rows = data[1:] if len(data) > 1 else []
            df_filtered = pd.DataFrame(rows, columns=headers)
        else:
            return pd.DataFrame()

        # BagKodu verilerini Google Sheets'ten oku için hazırlık - metin olarak sakla
        if 'BagKoduBekleyen' in df_filtered.columns:
            df_filtered['BagKoduBekleyen'] = df_filtered['BagKoduBekleyen'].astype(str)

        bagKodu_df['bagKodum'] = pd.to_numeric(bagKodu_df['bagKodum'], errors='coerce').fillna(0).astype(int).astype(str)

        # Birleştir ve sütunları genişlet
        merged_df = df_filtered.merge(bagKodu_df, left_on='BagKoduBekleyen', right_on='bagKodum', how='left')

        # Malzeme sütununu önce int sonra str türüne dönüştür ve koşullu sütunu ekle
        if 'Malzeme' in merged_df.columns:
            merged_df['Malzeme'] = merged_df['Malzeme'].astype(int).astype(str)
            merged_df['Malzeme Kodu'] = merged_df.apply(lambda row: f"{row['Malzeme']}-0" if pd.isna(row['malzemeKodu']) else row['malzemeKodu'], axis=1)

        # Kalem No için BagKoduBekleyen'i metin olarak ayarla
        merged_df['Kalem No'] = merged_df['BagKoduBekleyen'].astype(str)

        # Sipariş Miktarı sütununu önce düzelt (bin ayırıcı virgülleri kaldır ve 1000'e böl)
        if "Sipariş Miktarı" in merged_df.columns:
            merged_df["Sipariş Miktarı"] = merged_df["Sipariş Miktarı"].astype(str).str.replace(',', '', regex=False)
            merged_df["Sipariş Miktarı"] = pd.to_numeric(merged_df["Sipariş Miktarı"], errors='coerce').fillna(0)
            # 1000'e bölerek virgülden sonraki 3 sıfırı sil (14000 -> 14, 1000 -> 1)
            merged_df["Sipariş Miktarı"] = (merged_df["Sipariş Miktarı"] / 1000).astype(int)

        # Birim Fiyat güncellemesi için gerekli verileri al
        birim_fiyat_df = pd.DataFrame()
        required_cols = ["Sipariş Tarihi", "Malzeme", "Birim Fiyat"]

        # Birim Fiyat sütunu için alternatif isimleri kontrol et
        birim_fiyat_col = None
        possible_names = ["Birim Fiyat", "Birim Fiyatı", "BirimFiyat", "Fiyat", "Price", "Unit Price"]
        for col_name in possible_names:
            if col_name in merged_df.columns:
                birim_fiyat_col = col_name
                break

        # Global değişken tanımlaması fonksiyonun başında
        global bekleyen_birim_fiyat_df

        if birim_fiyat_col:
            required_cols = ["Sipariş Tarihi", "Malzeme", birim_fiyat_col]

            try:
                # Veri tiplerini düzenle
                merged_df["Sipariş Tarihi"] = pd.to_datetime(merged_df["Sipariş Tarihi"], errors='coerce')
                merged_df["Malzeme"] = merged_df["Malzeme"].astype(str)

                # Birim Fiyat için güvenli işleme
                merged_df[birim_fiyat_col] = (merged_df[birim_fiyat_col]
                    .astype(str)
                    .str.replace(',', '.', regex=False)
                    .replace(['', 'nan', 'NaN', 'null'], '0'))
                merged_df[birim_fiyat_col] = pd.to_numeric(merged_df[birim_fiyat_col], errors='coerce').fillna(0)

                # Geçerli verileri filtrele ve optimize et
                valid_data = merged_df[
                    (merged_df["Sipariş Tarihi"].notna()) &
                    (merged_df["Malzeme"].notna()) &
                    (merged_df[birim_fiyat_col] > 0)
                ][required_cols].copy()

                if not valid_data.empty:
                    # En yeni kaydı al (optimize edilmiş)
                    birim_fiyat_df = (valid_data
                        .sort_values("Sipariş Tarihi", ascending=False)
                        .drop_duplicates(subset=["Malzeme"], keep='first')
                        .reset_index(drop=True))

                    # Sütun adını standartlaştır
                    if birim_fiyat_col != "Birim Fiyat":
                        birim_fiyat_df.rename(columns={birim_fiyat_col: "Birim Fiyat"}, inplace=True)

                    # SAP kodu optimize edilmiş oluşturma
                    birim_fiyat_df['SAP_Match'] = birim_fiyat_df['Malzeme'].str[:10]

                    # Global değişken olarak kaydet
                    bekleyen_birim_fiyat_df = birim_fiyat_df
                else:
                    bekleyen_birim_fiyat_df = pd.DataFrame()

            except Exception as e:
                logger.error(f"Birim Fiyat verisi işleme hatası: {e}")
                bekleyen_birim_fiyat_df = pd.DataFrame()
        else:
            bekleyen_birim_fiyat_df = pd.DataFrame()

        sirali_df = merged_df.rename(columns={
            "Malzeme kısa metni": "Ürün Adı",
            "Spec Adı": "Spec Adı",
            "Sipariş Miktarı": "Bekleyen Adet",
            "Sipariş Durum Tanım": "Durum",
            "Teslimat tarihi": "Teslimat Tarihi",
            "Depo Yeri": "Depo Yeri Plaka",
            "Teslim Deposu": "Teslim Deposu"
        }).reindex(columns=["Sipariş Tarihi", "Kalem No", "Ürün Adı", "Spec Adı", "Bekleyen Adet", "Durum", "Teslimat Tarihi", "Depo Yeri Plaka", "Teslim Deposu","Malzeme Kodu","KDV(%)","Prosap Sözleşme Ad Soyad"])

        # Durum sütununu güncelle - "ACIK" -> "Açık", "SEVK" -> "Sevke Hazır", "URET" -> "Üretiliyor", diğerleri -> "Açık"
        if "Durum" in sirali_df.columns:
            sirali_df["Durum"] = sirali_df["Durum"].apply(
                lambda x: "Açık" if str(x).upper() == "ACIK" else
                            "Sevke Hazır" if str(x).upper() == "SEVK" else
                            "Üretiliyor" if str(x).upper() == "URET" else
                            "Açık"  # Diğer tüm durumlar için varsayılan
            )

        return sirali_df

    except Exception as e:
        logger.error(f"Bekleyen sipariş DataFrame işleme hatası: {e}")
        return pd.DataFrame()

# Global değişken tanımlaması
bekleyen_birim_fiyat_df = pd.DataFrame()

# ============================================================================
# MAIN DATA PROCESSING FUNCTION
# ============================================================================

def stok_verilerini_duzenle_ve_kaydet(malzeme, bekleyen, malzeme_toplam_borc, plan, liste, ayar):
    """
    [ADIM 3.1] Multi-Source Veri Birleştirme ve Final Stok Analizi

    Tüm veri kaynaklarını malzeme kodu bazlı olarak birleştirerek
    comprehensive stok durumu analizi yapar ve optimize edilmiş
    final stok raporu oluşturur.

    Veri Birleştirme Stratejisi:
    1. Ana kaynak: Malzeme master data (LEFT JOIN base)
    2. Bekleyen siparişler: GROUP BY malzeme kodu, SUM bekleyen adet
    3. Toplam borç: Sevkiyat borç toplamları
    4. Plan verisi: PRGsheets/Plan sayfasından alınan plan verileri
    5. Fiyat listesi: SAP kodları ve fiyat bilgileri

    Critical Business Logic - Stok Hesaplama Algoritması:

    FAZLA = (DEPO + Bekleyen + Plan - Borç)
    - Eğer sonuc > 0: Fazla stok var
    - Eğer sonuc <= 0: 0 (fazla yok)

    VER = (Borç - DEPO - Bekleyen - Plan)
    - Eğer sonuc > 0: Bu kadar malzeme tedarik edilmeli
    - Eğer sonuc <= 0: 0 (tedarik gerekmez)

    Args:
        malzeme (pd.DataFrame): Master malzeme bilgileri
        bekleyen (pd.DataFrame): İşlenmiş bekleyen sipariş verileri
        malzeme_toplam_borc (pd.DataFrame): Malzeme bazında toplam borç
        plan (pd.DataFrame): Plan verisi (PRGsheets/Plan sayfasından)
        liste (pd.DataFrame): Fiyat ve SAP kodu bilgileri
        ayar (dict): Ayar verileri (KDV ve Ön Ödeme İskonto)

    Returns:
        pd.DataFrame: Comprehensive final stok analiz raporu

    Output Columns (Optimize Edilmiş Sıralama):
    ['SAP Kodu', 'Ver', 'Sepet', 'Malzeme Adı', 'DEPO', 'Fazla', 'Borç',
     'Bekleyen', 'Plan', 'EXC', 'SUBE', 'ID1', 'ID2', '###', 'INDIRIM',
     'Malzeme Kodu', 'Miktar', 'TOPTAN', 'PERAKENDE', 'LISTE']
    """
    try:
        # Global değişken tanımlaması
        global bekleyen_birim_fiyat_df

        # Güvenli kopya oluştur ve sütun isimlerini düzenle
        malzeme = malzeme.copy()  # Orijinal veriyi koruma

        # Sütun yeniden adlandırma
        column_mapping = {
            'msg_S_0870': 'Malzeme Adı',
            'msg_S_0078': 'Malzeme Kodu',
            'EXCLUSIVE': 'EXC',
            'msg_S_0165': 'Miktar'
        }
        # Sadece mevcut sütunları yeniden adlandır
        existing_columns = {k: v for k, v in column_mapping.items() if k in malzeme.columns}
        if existing_columns:
            malzeme.rename(columns=existing_columns, inplace=True)

        # Gereksiz sütunları güvenli kaldırma
        gereksiz_sutunlar = ['msg_S_0088', 'SPEC', 'ZZ', 'ETIKET', 'SAYAÇ']
        columns_to_drop = [col for col in gereksiz_sutunlar if col in malzeme.columns]
        if columns_to_drop:
            malzeme.drop(columns=columns_to_drop, inplace=True)

        # Bekleyen siparişleri malzeme kodu bazında grupla
        grouped_bekleyen = bekleyen.groupby('Malzeme Kodu', as_index=False).agg({'Bekleyen Adet': 'sum'})

        # Ana malzeme DataFrame'i temel alarak diğer verileri birleştir
        merged_df = pd.merge(malzeme, grouped_bekleyen, on='Malzeme Kodu', how='left')
        merged_df['Bekleyen Adet'] = merged_df['Bekleyen Adet'].fillna(0)

        merged_df = pd.merge(merged_df, malzeme_toplam_borc, on='Malzeme Kodu', how='left')
        merged_df['Toplam Borç'] = merged_df['Toplam Borç'].fillna(0)

        merged_df = pd.merge(merged_df, plan, on='Malzeme Kodu', how='left')
        merged_df['Adet'] = merged_df['Adet'].fillna(0)

        # Malzeme Kodu'nun ilk 10 hanesini SAP Kodu olarak kaydet
        merged_df['SAP Kodu'] = merged_df['Malzeme Kodu'].astype(str).str[:10]

        # Fiyat listesi ile SAP Kodu eşleştirmesi yap
        merged_df = pd.merge(merged_df, liste[['SAP Kodu', 'TOPTAN', 'PERAKENDE', 'LISTE']],
                           left_on='SAP Kodu', right_on='SAP Kodu', how='left')

        # Eşleşmeyen değerler için 0 ata
        for col in ['TOPTAN', 'PERAKENDE', 'LISTE']:
            merged_df[col] = merged_df[col].fillna(0)

        # Veri tiplerini düzenle (sayısal sütunlar)
        numerik_sutunlar = ['DEPO', 'EXC', 'SUBE', 'Miktar', 'Bekleyen Adet', 'Toplam Borç', 'Adet', 'TOPTAN', 'PERAKENDE', 'LISTE', 'ID1', 'ID2', '###', 'INDIRIM']
        for sutun in numerik_sutunlar:
            if sutun in merged_df.columns:
                merged_df[sutun] = merged_df[sutun].fillna(0).astype(int)

        # String sütunları düzenle
        merged_df['Malzeme Kodu'] = merged_df['Malzeme Kodu'].astype(str)
        if 'SAP Kodu' in merged_df.columns:
            merged_df['SAP Kodu'] = merged_df['SAP Kodu'].astype(str)

        # Sütun isimlerini yeniden düzenle
        merged_df.rename(columns={
            'Bekleyen Adet': 'Bekleyen',
            'Toplam Borç': 'Borç',
            'Adet': 'Plan'
        }, inplace=True)

        # "Fazla" sütunu hesaplama (DEPO + Bekleyen + Plan - Borç)
        # Eğer sonuç <= 0 ise 0, aksi halde hesaplanan değer
        fazla_hesaplama = merged_df['DEPO'] + merged_df['Bekleyen'] + merged_df['Plan'] - merged_df['Borç']
        merged_df['Fazla'] = fazla_hesaplama.where(fazla_hesaplama > 0, 0)

        # "Ver" sütunu hesaplama (Borç - DEPO - Bekleyen - Plan)
        # Eğer sonuç <= 0 ise 0, aksi halde hesaplanan değer
        ver_hesaplama = merged_df['Borç'] - merged_df['DEPO'] - merged_df['Bekleyen'] - merged_df['Plan']
        merged_df['Ver'] = ver_hesaplama.where(ver_hesaplama > 0, 0)

        # Sepet sütunu ekle (varsayılan 0)
        merged_df['Sepet'] = 0

        # Ayar değerlerini al
        kdv = ayar.get('KDV', 1.10)
        on_odeme_iskonto = ayar.get('Ön Ödeme İskonto', 0.90)

        # LAST sütunu güncelleme (bekleyenden gelen Birim Fiyat verileri ile)
        if not bekleyen_birim_fiyat_df.empty and 'LAST' in merged_df.columns:
            try:
                # Optimize edilmiş eşleştirme: pandas merge kullan
                # SAP kodlarını hazırla
                bekleyen_birim_fiyat_df['SAP_Match_Clean'] = bekleyen_birim_fiyat_df['SAP_Match'].str.strip()
                merged_df['SAP_Kodu_Clean'] = merged_df['SAP Kodu'].astype(str).str.strip()

                # Merge işlemi ile hızlı eşleştirme
                price_update = merged_df[['SAP_Kodu_Clean']].merge(
                    bekleyen_birim_fiyat_df[['SAP_Match_Clean', 'Birim Fiyat']],
                    left_on='SAP_Kodu_Clean',
                    right_on='SAP_Match_Clean',
                    how='left'
                )

                # LAST sütununu güncelle
                mask = (price_update['Birim Fiyat'].notna()) & (price_update['Birim Fiyat'] > 0)
                merged_df.loc[mask, 'LAST'] = price_update.loc[mask, 'Birim Fiyat']

                # Temizlik
                merged_df.drop('SAP_Kodu_Clean', axis=1, inplace=True)

            except Exception as e:
                logger.error(f"LAST güncelleme hatası: {e}")

        # ID1 = LAST * KDV hesaplaması (LAST sütunu varsa)
        if 'LAST' in merged_df.columns:
            merged_df['ID1'] = merged_df['LAST'].apply(lambda x: int(x * kdv) if pd.notna(x) and x != 0 else 0)
        else:
            merged_df['ID1'] = 0

        # ID2 = TOPTAN * KDV * Ön Ödeme İskonto hesaplaması
        merged_df['ID2'] = merged_df['TOPTAN'].apply(lambda x: int(x * kdv * on_odeme_iskonto) if pd.notna(x) and x != 0 else 0)

        # ### hesaplaması: 1-(ID2/PERAKENDE)*100 (PERAKENDE=0 ise 0)
        merged_df['###'] = merged_df.apply(
            lambda row: int((1 - (row['ID2'] / row['PERAKENDE'])) * 100)
            if row['PERAKENDE'] != 0
            else 0,
            axis=1
        )

        # INDIRIM hesaplaması: 1-(PERAKENDE/LISTE)*100
        merged_df['INDIRIM'] = merged_df.apply(
            lambda row: int((1 - (row['PERAKENDE'] / row['LISTE'])) * 100)
            if row['LISTE'] != 0
            else 0,
            axis=1
        )

        # Final sütun sıralaması - önce mevcut sütunları kontrol et
        available_columns = merged_df.columns.tolist()
        desired_columns = [
            "SAP Kodu", "Ver", "Sepet", "Malzeme Adı", "DEPO", "Fazla", "Borç",
            "Bekleyen", "Plan", "EXC", "SUBE", "ID1","ID2", "###", "INDIRIM",
            "Malzeme Kodu", "Miktar", "PERAKENDE", "LISTE"
        ]

        # Sadece mevcut sütunları seç
        final_columns = [col for col in desired_columns if col in available_columns]
        merged_df = merged_df[final_columns]

        # Güvenlik kontrolleri
        if merged_df.empty:
            logger.warning("Final stok DataFrame boş!")
            return pd.DataFrame()

        # Son kontrol: gerekli sütunların varlığını doğrula
        required_final_cols = ['SAP Kodu', 'Malzeme Adı', 'Malzeme Kodu']
        missing_cols = [col for col in required_final_cols if col not in merged_df.columns]
        if missing_cols:
            logger.error(f"Kritik sütunlar eksik: {missing_cols}")
            return pd.DataFrame()

        return merged_df

    except Exception as e:
        logger.error(f"❌ Stok verilerini düzenleme hatası: {e}")
        return pd.DataFrame()

# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    """
    [ANA KONTROLÇÜ] Stok Yönetim Sistemi Orchestration Engine - Service Account

    Tüm stok yönetimi işlemlerini belirlenen sırada koordine eder
    ve Google Sheets ile entegre final raporlama sağlar.

    İşlem Akışı (Sequential Execution):

    PHASE 1 - Veri Çekimi:
      1.1. Sevkiyat borç verilerini çek (SQL SP)
      1.2. Master malzeme listesini çek (SQL)
      1.3. Barkod bilgilerini çek (SQL JOIN)

    PHASE 2 - Veri İşleme:
      2.1. Toplam borç hesaplama (GROUP BY malzeme)
      2.2. Bekleyen sipariş Excel'i işle (merge + transform)

    PHASE 3 - Final Analiz:
      3.1. Multi-source data merging
      3.2. Stok hesaplama algoritmaları (Fazla/Ver)
      3.3. Google Sheets'e otomatik upload

    Error Handling:
    - Comprehensive logging (file + console)
    - Graceful degradation (eksik dosyalar için boş DataFrame)
    - Database connection failover

    Output:
    - Google Sheets: PRGsheets/Stok sayfası
    - Log file: stok_islemleri.log
    """
    logger.info("Stok Yonetim Sistemi Baslatilyior...")
    logger.info("=" * 50)

    try:
        # Global değişken tanımlaması
        global bekleyen_birim_fiyat_df

        # Config yükle (Service Account otomatik başlar)
        config = StokConfig()

        # Google Sheets yöneticisini başlat
        sheets_yoneticisi = None
        try:
            sheets_yoneticisi = GoogleSheetsYoneticisi(config)
            logger.info("Google Sheets baglantisi kuruldu")
        except Exception as e:
            logger.error(f"Google Sheets baglantisi kurulamadi: {e}")
            sheets_yoneticisi = None

        # PHASE 1: ERP SİSTEMİNDEN VERİ ÇEKİMİ
        logger.info("PHASE 1: ERP sisteminden master data çekimi başlatılıyor...")
        logger.info("-" * 60)

        # 1.1. Sevkiyat borç analizi (Stored Procedure)
        logger.info("1.1. Sevkiyat borç analizi çalıştırılıyor (SP)...")
        cari_sevkiyat_df = cari_sevkiyat_borcu_al(config)
        logger.info(f"     ✓ Sevkiyat borç kayıtları: {len(cari_sevkiyat_df):,}")

        # 1.2. Master malzeme listesi (STOKLAR_CHOOSE_3A)
        logger.info("1.2. Master malzeme listesi çekiliyor...")
        malzeme_df = malzeme_listesini_al(config)
        logger.info(f"     ✓ Malzeme master kayıtları: {len(malzeme_df):,}")

        # 1.3. Barkod-malzeme eşleştirme matrisi
        logger.info("1.3. Barkod eşleştirme matrisi çekiliyor...")
        barkod_df = barkod_bilgilerini_al(config)
        logger.info(f"     ✓ Barkod eşleştirme kayıtları: {len(barkod_df):,}")

        # PHASE 2: VERİ İŞLEME VE HESAPLAMALAR
        logger.info("")
        logger.info("PHASE 2: Veri işleme ve business logic hesaplamaları...")
        logger.info("-" * 60)

        # 2.1. Malzeme bazında toplam borç aggregation
        logger.info("2.1. Malzeme bazında toplam borç hesaplama (GROUP BY)...")
        toplam_borc_df = pd.DataFrame()
        if not cari_sevkiyat_df.empty:
            toplam_borc_df = cari_sevkiyat_df.groupby('Malzeme Kodu')['Kalan Siparis'].sum().reset_index()
            toplam_borc_df.rename(columns={'Kalan Siparis': 'Toplam Borç'}, inplace=True)
            logger.info(f"     ✓ Agregasyon tamamlandı: {len(toplam_borc_df):,} malzeme")
        else:
            logger.warning("     ⚠ Sevkiyat borcu verisi boş - toplam borç hesaplanamadı")

        # 2.2. Bekleyen sipariş Google Sheets processing ve barkod eşleştirmesi
        logger.info("2.2. Bekleyen sipariş Google Sheets işleme (barkod matching)...")
        bekleyen_df = pd.DataFrame()

        if sheets_yoneticisi and not barkod_df.empty:
            try:
                bekleyen_df = bekleyen_siparisleri_isle_dataframe(sheets_yoneticisi, barkod_df)
                logger.info(f"     ✓ Bekleyen sipariş kayıtları: {len(bekleyen_df):,}")
            except Exception as e:
                logger.warning(f"     ⚠ PRGsheets/Bekleyen sayfası okunamadı: {e}")
        else:
            # Boş global değişken ata
            bekleyen_birim_fiyat_df = pd.DataFrame()

        logger.info("     ✓ Veri işleme tamamlandı")

        # PHASE 3: FİNAL ANALİZ VE RAPORLAMA
        logger.info("")
        logger.info("PHASE 3: Multi-source data integration ve final analiz...")
        logger.info("-" * 60)

        # 3.1. Auxiliary data yükleme (plan, fiyat listesi ve ayarlar)
        logger.info("3.1. Auxiliary data sources yükleniyor...")

        # Ayar verisi (PRGsheets/Ayar sayfasından)
        ayar_dict = {'KDV': 1.10, 'Ön Ödeme İskonto': 0.90}  # Varsayılan değerler
        if sheets_yoneticisi:
            try:
                ayar_dict = ayar_verilerini_al(sheets_yoneticisi)
                logger.info(f"     ✓ Ayar verileri yüklendi")
            except Exception as e:
                logger.warning(f"     ⚠ Ayar verisi okuma hatası: {e}")
        else:
            logger.info("     ✓ Google Sheets bağlantısı yok - varsayılan ayar değerleri kullanılıyor")

        # Plan verisi (PRGsheets/Plan sayfasından)
        plan_df = pd.DataFrame(columns=['Malzeme Kodu', 'Adet'])
        if sheets_yoneticisi:
            try:
                plan_df = plan_verilerini_al(sheets_yoneticisi)
                logger.info(f"     ✓ Plan verisi yüklendi: {len(plan_df):,} kayıt")
            except Exception as e:
                logger.warning(f"     ⚠ Plan verisi okuma hatası: {e}")
        else:
            logger.info("     ✓ Google Sheets bağlantısı yok - boş plan DataFrame kullanılıyor")

        # Fiyat listesi (PRGsheets/Fiyat sayfasından)
        liste_df = pd.DataFrame(columns=['SAP Kodu', 'TOPTAN', 'PERAKENDE', 'LISTE'])
        if sheets_yoneticisi:
            try:
                liste_df = fiyat_verilerini_al(sheets_yoneticisi)
                logger.info(f"     ✓ Fiyat listesi yüklendi: {len(liste_df):,} kayıt")
            except Exception as e:
                logger.warning(f"     ⚠ Fiyat verisi okuma hatası: {e}")
        else:
            logger.info("     ✓ Google Sheets bağlantısı yok - boş fiyat DataFrame kullanılıyor")

        # 3.2. Comprehensive stok analizi ve hesaplama algoritması
        logger.info("3.2. Multi-source integration ve stok algoritması çalıştırılıyor...")
        final_stok_df = stok_verilerini_duzenle_ve_kaydet(
            malzeme_df,       # Master malzeme data
            bekleyen_df,      # İşlenmiş bekleyen siparişler
            toplam_borc_df,   # Aggregated borç toplamları
            plan_df,          # Plan verisi (PRGsheets/Plan sayfasından)
            liste_df,         # SAP kodları ve fiyatlar
            ayar_dict         # Ayar verileri (KDV ve Ön Ödeme İskonto)
        )

        # 3.3. Google Sheets final raporlama
        logger.info("3.3. Final stok raporu Google Sheets'e aktarılıyor...")
        if not final_stok_df.empty and sheets_yoneticisi:
            sheets_yoneticisi.sayfa_guncelle("Stok", final_stok_df)
            logger.info(f"     ✓ Google Sheets güncellemesi başarılı: {len(final_stok_df):,} kayıt")
            logger.info("     ✓ Hedef: PRGsheets/Stok sayfası")
        else:
            logger.warning("     ⚠ Google Sheets güncellenemedi (bağlantı veya veri eksik)")

        # SİSTEM TAMAMLANDI
        logger.info("🎆 STOK YÖNETİM SİSTEMİ TAMAMLANDI!")
        return True

    except Exception as e:
        logger.error(f"❌ SİSTEM HATASI: {e}")
        # Hata detaylarını loglama
        import traceback
        logger.error(f"Hata detayı: {traceback.format_exc()}")
        return False  # Graceful exit

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)  # Başarılı çıkış
        else:
            sys.exit(1)  # Hata ile çıkış
    except KeyboardInterrupt:
        logger.warning("\n⚡ İşlem kullanıcı tarafından iptal edildi")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Beklenmeyen hata: {e}")
        sys.exit(1)
