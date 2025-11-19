#!/usr/bin/env python3
"""
Sevkiyat Borç Analizi ve Google Sheets Entegrasyonu - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli sevkiyat analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Server entegrasyonu
- Cari/Borç/Malzeme/Plan/Bekleyenler veri işleme
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import os
import sys
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pyodbc

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# PyInstaller ile freeze edildiğinde dosya yollarını düzelt
if getattr(sys, 'frozen', False):
    # Exe olarak çalışıyorsa: exe'nin bulunduğu dizini kullan
    base_dir = Path(sys.executable).parent
else:
    # Normal Python olarak çalışıyorsa: script dizinini kullan
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'sevkiyat_analizi.log'

logging.basicConfig(
    level=logging.ERROR,  # Sadece hatalar
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Service Account ve Merkezi Config
# ============================================================================

class SevkiyatConfig:
    """Configuration management - Service Account kullanır"""

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()

            # PRGsheet'ten ayarları yükle
            self.settings = self.config_manager.get_settings()

            logger.info("Config yüklendi")

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            raise

    @property
    def database(self) -> 'DatabaseConfig':
        """Database configuration"""
        return DatabaseConfig(
            server=self.settings.get('SQL_SERVER', ''),
            database=self.settings.get('SQL_DATABASE', ''),
            username=self.settings.get('SQL_USERNAME', ''),
            password=self.settings.get('SQL_PASSWORD', '')
        )

    @property
    def google_sheets(self) -> 'GoogleSheetsConfig':
        """Google Sheets configuration"""
        return GoogleSheetsConfig(
            config_manager=self.config_manager,
            spreadsheet_id=self.config_manager.MASTER_SPREADSHEET_ID
        )

@dataclass(frozen=True)
class DatabaseConfig:
    """SQL Server bağlantı konfigürasyonu."""
    server: str
    database: str
    username: str
    password: str
    driver: str = 'ODBC Driver 17 for SQL Server'

    @property
    def connection_string(self) -> str:
        """ODBC bağlantı string'ini döndürür."""
        if not all([self.server, self.database, self.username, self.password]):
            raise ValueError(
                "PRGsheet → Ayar sayfasında eksik SQL ayarları!\n"
                "Gerekli: SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD"
            )

        return (
            f'DRIVER={{{self.driver}}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )

@dataclass(frozen=True)
class GoogleSheetsConfig:
    """Google Sheets API konfigürasyonu - Service Account."""
    config_manager: CentralConfigManager
    spreadsheet_id: str
    worksheet_name: str = 'Sevkiyat'

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """SQL Server veritabanı işlemlerini yöneten sınıf."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._test_connection()

    def _test_connection(self) -> None:
        """Bağlantıyı test eder."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            logger.info("Database bağlantı testi başarılı")
        except Exception as e:
            logger.error(f"Database bağlantı testi başarısız: {e}")
            raise ConnectionError(f"SQL Server bağlantısı kurulamadı: {e}")

    @contextmanager
    def get_connection(self):
        """Database bağlantısı context manager'ı."""
        connection = None
        try:
            connection = pyodbc.connect(
                self.config.connection_string,
                timeout=30
            )
            connection.timeout = 300  # 5 dakika sorgu timeout
            logger.debug("Database bağlantısı kuruldu")
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database bağlantı hatası: {e}")
            raise
        except Exception as e:
            logger.error(f"Beklenmeyen database hatası: {e}")
            raise
        finally:
            if connection:
                try:
                    connection.close()
                    logger.debug("Database bağlantısı kapatıldı")
                except Exception as e:
                    logger.warning(f"Database bağlantısı kapatılırken hata: {e}")

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """Google Sheets API işlemlerini yöneten sınıf - Service Account."""

    def __init__(self, config: GoogleSheetsConfig):
        self.config = config
        self.gc = config.config_manager.gc  # Service Account ile yetkilendirilmiş client
        self._test_connection()

    def _test_connection(self) -> None:
        """Google Sheets bağlantısını test eder."""
        try:
            spreadsheet = self.gc.open_by_key(self.config.spreadsheet_id)
            logger.info(f"Google Sheets bağlantı testi başarılı: {spreadsheet.title}")
        except Exception as e:
            logger.error(f"Google Sheets bağlantı testi başarısız: {e}")
            raise ConnectionError(f"Spreadsheet açılamadı: {e}")

    def update_worksheet(self, data: pd.DataFrame, worksheet_name: str = None, clear_if_empty: bool = True) -> None:
        """Worksheet'i verilen data ile günceller.

        Args:
            data: Yüklenecek DataFrame verisi
            worksheet_name: Hedef worksheet adı (None ise config'den alınır)
            clear_if_empty: True ise boş veri geldiğinde worksheet temizlenir
        """
        # Worksheet adını belirle
        target_worksheet = worksheet_name or self.config.worksheet_name

        # Boş veri kontrolü - eğer clear_if_empty=True ise sütun adlarını koru
        if data.empty:
            if clear_if_empty:
                logger.warning(f"{target_worksheet} için veri yok, sadece başlıklar korunuyor")
                try:
                    spreadsheet = self.gc.open_by_key(self.config.spreadsheet_id)
                    worksheet = self._get_or_create_worksheet(spreadsheet, target_worksheet)

                    # Mevcut başlıkları al (ilk satır)
                    try:
                        existing_headers = worksheet.row_values(1)
                        if existing_headers:
                            # Worksheet'i temizle
                            worksheet.clear()
                            # Sadece başlıkları geri yaz
                            worksheet.update(values=[existing_headers], range_name='A1')
                            logger.info(f"{target_worksheet} worksheet temizlendi, başlıklar korundu: {existing_headers}")
                        else:
                            # Başlık yoksa tamamen temizle
                            worksheet.clear()
                            logger.info(f"{target_worksheet} worksheet tamamen temizlendi")
                    except Exception as header_error:
                        logger.warning(f"Başlık alınamadı, worksheet tamamen temizleniyor: {header_error}")
                        worksheet.clear()

                except Exception as e:
                    logger.error(f"{target_worksheet} worksheet temizleme hatası: {e}")
            else:
                logger.warning(f"{target_worksheet} için veri yok, işlem yapılmadı")
            return

        try:
            spreadsheet = self.gc.open_by_key(self.config.spreadsheet_id)
            worksheet = self._get_or_create_worksheet(spreadsheet, target_worksheet)

            # Worksheet'i temizle
            worksheet.clear()
            logger.info(f"{target_worksheet} worksheet temizlendi")

            # Veriyi hazırla ve yükle
            values = self._prepare_data_for_sheets(data)

            # Batch update kullanarak performansı artır
            worksheet.update(values=values, range_name='A1', value_input_option='USER_ENTERED')

            logger.info(
                f"{target_worksheet} worksheet güncellendi: "
                f"{len(data)} satır, {len(data.columns)} sütun"
            )

        except Exception as e:
            logger.error(f"Worksheet güncelleme hatası: {e}")
            raise

    def _get_or_create_worksheet(self, spreadsheet, worksheet_name: str = None):
        """Worksheet'i getirir, yoksa oluşturur."""
        target_name = worksheet_name or self.config.worksheet_name
        try:
            worksheet = spreadsheet.worksheet(target_name)
            logger.debug(f"Mevcut worksheet bulundu: {target_name}")
            return worksheet
        except:
            logger.info(f"Worksheet bulunamadı, yeni oluşturuluyor: {target_name}")
            worksheet = spreadsheet.add_worksheet(
                title=target_name,
                rows=1000,
                cols=20
            )
            return worksheet

    def _prepare_data_for_sheets(self, data: pd.DataFrame) -> List[List]:
        """DataFrame'i Google Sheets formatına çevirir."""
        # Başlıkları ekle
        values = [data.columns.tolist()]

        # Özel sütunların indekslerini bul
        cari_kodu_index = None
        kalem_no_index = None
        if 'Cari Kodu' in data.columns:
            cari_kodu_index = data.columns.get_loc('Cari Kodu')
        if 'Kalem No' in data.columns:
            kalem_no_index = data.columns.get_loc('Kalem No')

        # Veriyi satır satır ekle, None değerleri boş string yap
        for _, row in data.iterrows():
            row_values = []
            for col_index, value in enumerate(row):
                if pd.isna(value) or value is None:
                    row_values.append('')
                elif col_index == cari_kodu_index:
                    # Cari Kodu için özel işlem - text olarak formatla
                    row_values.append(f"'{str(value)}")  # Apostrofla başlat
                elif col_index == kalem_no_index:
                    # Kalem No için özel işlem - büyük sayıları text olarak formatla
                    row_values.append(f"'{str(value)}")  # Apostrofla başlat
                elif isinstance(value, (int, float)):
                    row_values.append(value)
                else:
                    row_values.append(str(value))
            values.append(row_values)

        return values

# ============================================================================
# SEVKIYAT DATA PROCESSOR
# ============================================================================

class SevkiyatDataProcessor:
    """Sevkiyat verilerini işleyen sınıf."""

    # DEPO türleri - bu depolar için kalan sipariş sıfırlanır
    ZERO_ORDER_DEPOTS = {'SUBE', 'EXC'}

    def __init__(self, db_manager: DatabaseManager, config_manager: CentralConfigManager):
        self.db_manager = db_manager
        self.config_manager = config_manager
        self.gc = config_manager.gc

    def extract_cari_data(self, sevkiyat_df: pd.DataFrame) -> pd.DataFrame:
        """Sevkiyat verisinden cari bilgilerini çıkarır ve detaylandırır."""
        if sevkiyat_df.empty:
            logger.warning("Sevkiyat verisi boş, cari verisi çıkarılamıyor")
            return pd.DataFrame()

        try:
            # Cari Kodu'na göre filtreleme ve Cari Adi sütununu alıp kaydetme
            filtered_df = sevkiyat_df[['Cari Adi', 'Cari Kodu']].drop_duplicates()
            logger.info(f"Benzersiz cari kayıt sayısı: {len(filtered_df)}")

            # SQL'den cari detay bilgilerini çek
            cari_details_df = self._get_cari_details()

            if cari_details_df.empty:
                logger.warning("Cari detay bilgileri çekilemedi")
                return filtered_df

            # Veri tipi tutarlılığını sağla - Cari Kodu'nu string yap
            filtered_df['Cari Kodu'] = filtered_df['Cari Kodu'].astype(str)
            cari_details_df['cariKod'] = cari_details_df['cariKod'].astype(str)

            # filtered_df ile eşleşen cariTelefon bilgilerini seç
            merged_df = pd.merge(
                filtered_df,
                cari_details_df[['cariKod', 'cariTelefon', 'cariBakiye']],
                left_on='Cari Kodu',
                right_on='cariKod',
                how='left'
            )

            # Gereksiz sütunu kaldır
            merged_df = merged_df.drop(columns=['cariKod'])

            # Sütun sırasını düzenle
            merged_df = merged_df[['Cari Adi', 'Cari Kodu', 'cariTelefon']]

            # Risk sayfasından risk verilerini çek ve birleştir
            risk_data = self._get_risk_data()
            if not risk_data.empty:
                # Veri tipi tutarlılığını sağla - Risk sayfasındaki Cari hesap kodu'nu string yap
                if 'Cari hesap kodu' in risk_data.columns:
                    risk_data['Cari hesap kodu'] = risk_data['Cari hesap kodu'].astype(str)
                    merged_df['Cari Kodu'] = merged_df['Cari Kodu'].astype(str)

                    # Cari Kodu ile Risk sayfasındaki "Cari hesap kodu" eşleştirmesi
                    merged_df = pd.merge(
                        merged_df,
                        risk_data[['Cari hesap kodu', 'Risk']],
                        left_on='Cari Kodu',
                        right_on='Cari hesap kodu',
                        how='left'
                    )
                    # Gereksiz sütunu kaldır
                    merged_df = merged_df.drop(columns=['Cari hesap kodu'])
                    # Risk sütununu Bakiye olarak yeniden adlandır
                    merged_df = merged_df.rename(columns={'Risk': 'Bakiye'})
                    logger.info("Risk sayfası verileri ile eşleştirme tamamlandı")
                else:
                    logger.warning("Risk sayfasında 'Cari hesap kodu' sütunu bulunamadı")
                    merged_df['Bakiye'] = ''
            else:
                # Risk verisi yoksa Bakiye sütununu boş bırak
                merged_df['Bakiye'] = ''
                logger.warning("Risk sayfası verisi bulunamadı, Bakiye sütunu boş bırakıldı")

            # Sütun adlarını Türkçeleştir
            merged_df = merged_df.rename(columns={
                'Cari Adi': 'Cari Adı',
                'cariTelefon': 'Telefon'
            })

            logger.info(f"Cari verileri birleştirildi: {len(merged_df)} kayıt")
            return merged_df

        except Exception as e:
            logger.error(f"Cari veri çıkarma hatası: {e}")
            raise

    def create_borc_data(self, sevkiyat_df: pd.DataFrame) -> pd.DataFrame:
        """Sevkiyat verisinden malzeme kodu bazında borç verilerini oluşturur."""
        if sevkiyat_df.empty:
            logger.warning("Sevkiyat verisi boş, borç verisi oluşturulamıyor")
            return pd.DataFrame()

        try:
            # Malzeme Kodu'na göre filtreleme ve gruplandırma
            grouped_df = sevkiyat_df.groupby('Malzeme Kodu')['Kalan Siparis'].sum().reset_index()
            grouped_df.rename(columns={'Kalan Siparis': 'Toplam Borç'}, inplace=True)

            logger.info(f"Malzeme borç verileri oluşturuldu: {len(grouped_df)} kayıt")
            return grouped_df

        except Exception as e:
            logger.error(f"Borç veri oluşturma hatası: {e}")
            raise

    def _get_cari_details(self) -> pd.DataFrame:
        """SQL'den cari detay bilgilerini çeker."""
        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()

                # SQL sorgusunu tanımla
                sql_query = "SELECT * FROM CARI_HESAPLAR_CHOOSE_3A ORDER BY [cariAdi] ASC"

                # SQL sorgusunu çalıştır
                cursor.execute(sql_query)

                # Sonuçları al
                rows = cursor.fetchall()

                if not rows:
                    logger.warning("Cari hesaplar tablosundan veri dönemedi")
                    return pd.DataFrame()

                # Sonuçları pandas DataFrame'e dönüştür
                columns = [column[0] for column in cursor.description]
                cari_df = pd.DataFrame.from_records(rows, columns=columns)

                logger.info(f"Cari detay bilgileri çekildi: {len(cari_df)} kayıt")
                return cari_df

            except pyodbc.Error as e:
                logger.error(f"Cari detay SQL hatası: {e}")
                raise
            except Exception as e:
                logger.error(f"Cari detay çekme hatası: {e}")
                raise

    def _get_risk_data(self) -> pd.DataFrame:
        """Google Sheets'ten Risk sayfasından veri çeker."""
        try:
            # PRGsheet'i aç
            spreadsheet = self.gc.open_by_key(self.config_manager.MASTER_SPREADSHEET_ID)

            try:
                risk_worksheet = spreadsheet.worksheet('Risk')
                risk_data = risk_worksheet.get_all_records()

                if not risk_data:
                    logger.warning("Risk sayfasında veri yok")
                    return pd.DataFrame()

                risk_df = pd.DataFrame(risk_data)
                logger.info(f"Risk sayfasından {len(risk_df)} kayıt okundu")
                return risk_df

            except:
                logger.warning("Risk sayfası bulunamadı")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Risk sayfası okuma hatası: {e}")
            return pd.DataFrame()

    def _get_malzeme_data(self) -> pd.DataFrame:
        """SQL'den malzeme stok bilgilerini çeker."""
        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()

                # SQL sorgusunu tanımla
                sql_query = """SELECT [msg_S_0088] as  kayitNo
                    ,[msg_S_0870] as malzemeAdi
                    ,[SPEC]
                    ,[msg_S_0078] as malzemeKodu
                    ,[DEPO]
                    ,[EXCLUSIVE] as EXC
                    ,[SUBE] as SUBE
                    ,[msg_S_0165] as MIKTAR
                    ,[SAYAÇ] as SAYAC
                    ,[LAST]
                FROM [dbo].[STOKLAR_CHOOSE_3A] ORDER BY SAYAC DESC
                """

                # SQL sorgusunu çalıştır
                cursor.execute(sql_query)

                # Sonuçları al
                rows = cursor.fetchall()

                if not rows:
                    logger.warning("STOKLAR_CHOOSE_3A tablosundan veri dönemedi")
                    return pd.DataFrame()

                # Sonuçları pandas DataFrame'e dönüştür
                columns = [column[0] for column in cursor.description]
                malzeme_df = pd.DataFrame.from_records(rows, columns=columns)

                logger.info(f"Malzeme stok bilgileri çekildi: {len(malzeme_df)} kayıt")
                return malzeme_df

            except pyodbc.Error as e:
                logger.error(f"Malzeme stok SQL hatası: {e}")
                raise
            except Exception as e:
                logger.error(f"Malzeme stok çekme hatası: {e}")
                raise

    def _get_bekleyen_data(self) -> pd.DataFrame:
        """PRGsheets dosyasının Bekleyen sayfasından veri okur."""
        try:
            # Google Sheets'ten Bekleyen sayfasını oku
            spreadsheet = self.gc.open_by_key(self.config_manager.MASTER_SPREADSHEET_ID)

            try:
                bekleyen_worksheet = spreadsheet.worksheet('Bekleyen')
                bekleyen_data = bekleyen_worksheet.get_all_records()

                if not bekleyen_data:
                    logger.warning("Bekleyen sayfasında veri yok")
                    return pd.DataFrame()

                bekleyen_df = pd.DataFrame(bekleyen_data)
                logger.info(f"Bekleyen sayfasından {len(bekleyen_df)} kayıt okundu")
                return bekleyen_df

            except:
                logger.warning("Bekleyen sayfası bulunamadı")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Bekleyen sayfası okuma hatası: {e}")
            return pd.DataFrame()

    def _get_bagkodu_data(self) -> pd.DataFrame:
        """SQL Server'dan BagKodu verilerini direkt çeker."""
        try:
            with self.db_manager.get_connection() as connection:
                cursor = connection.cursor()

                # BagKodu.py'deki query'yi kullan
                query = """
                    SELECT TOP 100 PERCENT
                        bar_RECno AS [barkodKayit], bar_serino_veya_bagkodu AS [bagKodum],
                        bar_stokkodu AS [malzemeKodu], sto_isim AS [malzemeAdi]
                    FROM dbo.BARKOD_TANIMLARI WITH (NOLOCK)
                    LEFT OUTER JOIN dbo.STOKLAR ON sto_kod = bar_stokkodu
                    WHERE sto_pasif_fl IS NULL or sto_pasif_fl=0
                    ORDER BY bar_RECno DESC
                """

                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    logger.warning("BARKOD_TANIMLARI tablosundan veri dönemedi")
                    return pd.DataFrame()

                # Sonuçları pandas DataFrame'e dönüştür
                columns = [column[0] for column in cursor.description]
                bagkodu_df = pd.DataFrame.from_records(rows, columns=columns)

                logger.info(f"BagKodu verileri SQL'den çekildi: {len(bagkodu_df)} kayıt")
                return bagkodu_df

        except Exception as e:
            logger.error(f"BagKodu SQL çekme hatası: {e}")
            return pd.DataFrame()

    def create_bekleyenler_data(self) -> pd.DataFrame:
        """Bekleyen verilerini işler ve Bekleyenler sayfası için hazırlar."""
        try:
            # Bekleyen.xlsx dosyasından veri oku
            df = self._get_bekleyen_data()
            if df.empty:
                logger.warning("Bekleyen verisi boş")
                return pd.DataFrame()

            # BagKodu verilerini Google Sheets'ten oku
            bagKodu_df = self._get_bagkodu_data()
            if bagKodu_df.empty:
                logger.warning("BagKodu verisi boş")
                return pd.DataFrame()

            # Filtrele ve sütunları işleme (BagKoduBekleyen zaten mevcut)
            df_filtered = df[df['Malzeme kısa metni'].notna()].copy()

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

            logger.info(f"Bekleyenler verisi oluşturuldu: {len(sirali_df)} kayıt")
            return sirali_df

        except Exception as e:
            logger.error(f"Bekleyenler veri oluşturma hatası: {e}")
            raise

    def _get_plan_raw_data(self) -> pd.DataFrame:
        """SQL'den plan için ham veri çeker (fn_StokHareketEvrakYonetimi fonksiyonu)."""
        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()

                # SQL sorgusunu tanımla - 5 parametre (13 integer eklendi)
                sql_query = """
                    SELECT * FROM [dbo].[fn_StokHareketEvrakYonetimi] (
                    '20250101'
                    ,'20771231'
                    ,N''
                    ,13)
                    """

                # SQL sorgusunu çalıştır
                cursor.execute(sql_query)

                # Sonuçları al
                rows = cursor.fetchall()

                if not rows:
                    logger.warning("fn_StokHareketEvrakYonetimi fonksiyonundan veri dönemedi")
                    return pd.DataFrame()

                # Sonuçları pandas DataFrame'e dönüştür
                columns = [column[0] for column in cursor.description]
                df = pd.DataFrame.from_records(rows, columns=columns)

                logger.info(f"Plan ham verisi çekildi: {len(df)} kayıt")
                return df

            except pyodbc.Error as e:
                logger.error(f"Plan ham veri SQL hatası: {e}")
                raise
            except Exception as e:
                logger.error(f"Plan ham veri çekme hatası: {e}")
                raise

    def _get_fatura_data(self) -> pd.DataFrame:
        """Fatura.xlsx dosyasından veri okur."""
        try:
            # Exe için: önce exe'nin yanında, sonra varsayılan konumda ara
            if getattr(sys, 'frozen', False):
                # PyInstaller ile derlenmişse
                exe_dir = os.path.dirname(sys.executable)
            else:
                # Normal Python çalıştırılıyorsa
                exe_dir = os.path.dirname(os.path.abspath(__file__))

            local_path = os.path.join(exe_dir, "Fatura.xlsx")
            default_path = "D:/GoogleDrive/PRG/Fatura.xlsx"

            if os.path.exists(local_path):
                file_path = local_path
            elif os.path.exists(default_path):
                file_path = default_path
            else:
                logger.error(f"Fatura.xlsx dosyası bulunamadı. Aranan konumlar: {local_path}, {default_path}")
                return pd.DataFrame()

            df_fatura = pd.read_excel(file_path, sheet_name="Sheet1")

            if df_fatura.empty:
                logger.warning("Fatura.xlsx dosyası boş")
                return pd.DataFrame()

            logger.info(f"Fatura.xlsx dosyasından {len(df_fatura)} kayıt okundu")
            return df_fatura
        except Exception as e:
            logger.error(f"Fatura.xlsx dosyası okuma hatası: {e}")
            import traceback
            logger.error(f"Fatura.xlsx okuma detaylı hata: {traceback.format_exc()}")
            return pd.DataFrame()

    def create_plan_data(self) -> pd.DataFrame:
        """Plan verilerini işler ve Plan sayfası için hazırlar."""
        try:
            # Plan ham verilerini çek
            df = self._get_plan_raw_data()
            if df.empty:
                logger.error("Plan ham verisi boş - fn_StokHareketEvrakYonetimi'den veri gelmedi")
                return pd.DataFrame()

            logger.info(f"Plan ham verisi alındı: {len(df)} satır, sütunlar: {list(df.columns)}")

            # Kullanılmayan sütunları kaldır (sadece varsa)
            columns_to_drop = ["msg_S_0159", "msg_S_0555", "msg_S_0003", "msg_S_0077", "msg_S_0097", "msg_S_0404", "#msg_S_1007", "#msg_S_1009", "#msg_S_1010", "#msg_S_1011", "#msg_S_1012", "#msg_S_1013", "#msg_S_1014", "#msg_S_1015", "#msg_S_1016", "#msg_S_1017", "msg_S_0088", "msg_S_0090", "msg_S_0199", "msg_S_0200", "msg_S_0201", "TUTAR"]
            existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
            if existing_cols_to_drop:
                df.drop(columns=existing_cols_to_drop, inplace=True)
                logger.info(f"Kaldırılan sütunlar: {existing_cols_to_drop}")

            # Kolon türlerini değiştir ve yeniden adlandır
            df["msg_S_0089"] = pd.to_datetime(df["msg_S_0089"])
            df.rename(columns={"msg_S_0157": "İrsaliye No", "msg_S_0089": "İrsaliye Tarihi", "Vergili TUTAR": "Irsaliye Tutarı"}, inplace=True)

            # Satırları sırala
            df.sort_values(by="İrsaliye Tarihi", ascending=False, inplace=True)

            # "İrsaliye No" sütununu stringe çevir ve "İrsaliye Nom" sütunu oluştur
            df["İrsaliye Nom"] = df["İrsaliye No"].astype(str)

            # "İrsaliye" sütununu oluştur ve koşullu olarak değer ata
            df["Irsaliye"] = df.apply(lambda x: "9000" + x["İrsaliye Nom"] if x["İrsaliye No"] < 1000000 else "900" + x["İrsaliye Nom"], axis=1)

            # "İrsaliye Nom" ve "İrsaliye No" sütunlarını düşür
            df.drop(columns=["İrsaliye Nom", "İrsaliye No"], inplace=True)

            # "Irsaliye" sütununu stringe çevir
            df["Irsaliye"] = df["Irsaliye"].astype(str)

            # Fatura verisini oku
            df_fatura = self._get_fatura_data()
            if df_fatura.empty:
                logger.error("Fatura verisi boş - Plan verisi oluşturulamıyor (Fatura.xlsx kontrolü gerekli)")
                return pd.DataFrame()

            df_fatura["Fatura No"] = df_fatura["Fatura No"].astype(str)

            # Her string'den yüzde işareti sonrası kısmı alıp tamsayıya çevirmek
            yuzde = []
            for p in df_fatura["Vergi Sınıfı Tanımı"]:
                try:
                    if pd.notna(p) and len(str(p)) > 1:
                        yuzde.append(int(str(p)[1:]))
                    else:
                        yuzde.append(0)
                except (ValueError, IndexError):
                    yuzde.append(0)

            # Vergili Tutar sütununu ekleme
            import numpy as np
            yuzde_array = np.array(yuzde)
            df_fatura["Vergili Tutar"] = df_fatura["Net Tutar"] + df_fatura["Net Tutar"] * yuzde_array/100

            # Önce eksik değerleri kontrol ederek çıkaralım
            df_fatura_filtered = df_fatura[df_fatura["Prosap Sas Kalem no"].notna()]

            # Şimdi "Prosap Sas Kalem no" sütununda "S" ile başlamayanları alalım
            df_fatura_filtered = df_fatura_filtered[~df_fatura_filtered["Prosap Sas Kalem no"].str.startswith("S")]

            # İlk olarak negatif değerleri filtreliyoruz
            df_fatura_filtered_positive = df_fatura_filtered[df_fatura_filtered["Vergili Tutar"] >= 0]

            # Gruplama işlemini gerçekleştiriyoruz
            df_fatura_grouped = df_fatura_filtered_positive.groupby("Fatura No").agg({"Vergili Tutar": "sum"}).reset_index()

            # Kolon türünü değiştirme
            df_fatura_grouped["Fatura No"] = df_fatura_grouped["Fatura No"].astype(str)

            # İlk olarak, veri çerçevelerini birleştiriyoruz
            df_merged = pd.merge(df_fatura_grouped, df, left_on="Fatura No", right_on="Irsaliye", how="left")

            # Daha sonra, "Irsaliye" sütununun boş (NaN) olduğu satırları filtreliyoruz
            df_filtered = df_merged[df_merged["Irsaliye"].isna()]

            # df_filtered ve df_fatura'yı "Fatura No" sütununa göre eşleştirme
            final_df = pd.merge(df_filtered, df_fatura, on="Fatura No")

            # "Prosap Sas Kalem no" sütununu "-" karakterinden ikiye ayırarak "Satınalma No" ve "Kalem No" sütunlarını oluşturma
            split_cols = final_df["Prosap Sas Kalem no"].str.split("-", expand=True)

            if split_cols.shape[1] == 2:  # İki sütun olup olmadığını kontrol edin
                final_df[["Satınalma No", "Kalem No"]] = split_cols
            else:
                # Bölmenin iki sütun oluşturmadığı durumu yönetin
                # Örneğin, eksik sütunları `None` veya başka bir değerle doldurabilirsiniz
                split_cols = split_cols.reindex(columns=[0, 1], fill_value=None)
                final_df[["Satınalma No", "Kalem No"]] = split_cols

            # "Kalem No" sütununu tamsayıya çevirme - boş değerleri kontrol et
            final_df["Kalem No"] = pd.to_numeric(final_df["Kalem No"], errors='coerce').fillna(0).astype(int)
            # "Kalem - Metin" sütununu oluşturma
            final_df["Kalem - Metin"] = final_df["Kalem No"].astype(str)

            # "BagNo" sütununu oluşturma
            final_df["BagNo"] = final_df.apply(lambda x: "{}00{}".format(x["Satınalma No"], x["Kalem - Metin"]) if x["Kalem No"] >= 1000 else "{}0000{}".format(x["Satınalma No"], x["Kalem - Metin"]) if x["Kalem No"] < 100 else "{}000{}".format(x["Satınalma No"], x["Kalem - Metin"]), axis=1)

            # BagKodu verilerini SQL'den oku
            df_bag = self._get_bagkodu_data()
            if df_bag.empty:
                logger.error("BagKodu verisi boş - Plan verisi oluşturulamıyor (BARKOD_TANIMLARI tablosu kontrolü gerekli)")
                return pd.DataFrame()

            # "BagNo" ve "bagKodum" sütunlarını aynı türde olacak şekilde dönüştürme
            final_df["BagNo"] = final_df["BagNo"].astype(str)

            # İlk olarak, NaN değerler ve boş stringler için özel bir durum kontrolü yapıyoruz
            df_bag["bagKodum"] = df_bag["bagKodum"].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != '' else 'NaN')

            # final_df ve df_bag DataFrame'lerini "BagNo" ve "bagKodum" sütunlarına göre eşleştirme
            final_merged_df = pd.merge(final_df, df_bag, left_on="BagNo", right_on="bagKodum", how="left")

            # "malzemeKodu" sütununun boş değerlerini "Malzeme"-0 ile doldur
            final_merged_df["malzemeKodu"] = final_merged_df.apply(
                lambda row: f"{row['Malzeme']}-0" if pd.isna(row["malzemeKodu"]) else row["malzemeKodu"],
                axis=1
            )

            # İstenen sütunları tutup diğer sütunları silme
            desired_columns = ["Sipariş Tarihi", "Fatura No", "Malzeme Kısa Tanımı", "Spec", "Faturalanan Gerçek Miktar", "Nakliye Numarası", "Yaratılma Tarihi", "Depo Yeri", "Plaka", "malzemeKodu"]
            final_merged_df = final_merged_df[desired_columns]

            # Sütunları yeniden adlandırma
            final_merged_df.rename(columns={
                "malzemeKodu": "Malzeme Kodu",
                "Malzeme Kısa Tanımı": "Malzeme Adı",
                "Faturalanan Gerçek Miktar": "Adet",
                "Yaratılma Tarihi": "Sevk Tarihi"
            }, inplace=True)

            logger.info(f"Plan verisi oluşturuldu: {len(final_merged_df)} kayıt")
            return final_merged_df

        except Exception as e:
            logger.error(f"Plan veri oluşturma hatası: {e}")
            import traceback
            logger.error(f"Plan veri oluşturma detaylı hata: {traceback.format_exc()}")
            raise

    def extract_raw_data(self) -> pd.DataFrame:
        """SQL Server'dan ham veri çeker."""
        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()
                logger.info("Stored procedure çalıştırılıyor: sp_SiparisOperasyonlari")

                # SQL sorgusunu tanımla
                sql_query = """
                SET NOCOUNT ON;
                EXEC dbo.sp_SiparisOperasyonlari 0, '20230101', '20770717', 0, 0, 2, 1, 0, 0, N'', 1, N'', 0, 0, 0, 1
                """

                # SQL sorgusunu çalıştır
                cursor.execute(sql_query)

                # Sonuçları al
                rows = cursor.fetchall()

                if not rows:
                    logger.warning("Stored procedure'dan veri dönemedi")
                    return pd.DataFrame()

                # Sonuçları pandas DataFrame'e dönüştür
                columns = [column[0] for column in cursor.description]
                df = pd.DataFrame.from_records(rows, columns=columns)

                logger.info(f"Ham veri çekildi: {len(df)} satır, {len(df.columns)} sütun")
                return df

            except pyodbc.Error as e:
                logger.error(f"SQL sorgu hatası: {e}")
                raise
            except Exception as e:
                logger.error(f"Veri çekme hatası: {e}")
                raise

    def transform_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Ham veriyi işler ve dönüştürür."""
        if raw_df.empty:
            logger.warning("Dönüştürülecek veri yok")
            return pd.DataFrame()

        try:
            # Sadece gerekli sütunları seç ve adlarını değiştir
            df = raw_df[['msg_S_0463',
                         '#msg_S_0469', '#msg_S_0119',
                         '#msg_S_1130', '#msg_S_0260', 'msg_S_0159',
                         'msg_S_0201', 'msg_S_0200',
                         'msg_S_0157', 'msg_S_0241', '#msg_S_0005',
                         'msg_S_0070', 'msg_S_0078']].rename(columns={
                "msg_S_0241": "Tarih", "msg_S_0201": "Cari Adi", "msg_S_0070": "Malzeme Adı", "#msg_S_0005": "SPEC", "msg_S_0463": "Kalan Siparis",
                "msg_S_0159": "DEPO", "#msg_S_0469": "Toplam Stok", "#msg_S_1130": "Satıcı Adi", "#msg_S_0119": "Sorumluk Merkezi",
                "msg_S_0200": "Cari Kodu", "msg_S_0157": "Sipariş No", "msg_S_0078": "Malzeme Kodu", "#msg_S_0260": "Açıklama"
            })

            # Sütun sırasını belirle
            df = df.reindex(columns=["Tarih", "Cari Adi", "Malzeme Adı", "SPEC", "Kalan Siparis", "DEPO", "Toplam Stok", "Satıcı Adi", "Sorumluk Merkezi", "Cari Kodu", "Sipariş No", "Malzeme Kodu", "Açıklama"])
            # İş kurallarını uygula
            df = self._apply_business_rules(df)

            # Veri tiplerini düzenle
            df = self._normalize_data_types(df)

            logger.info(f"Veri dönüştürüldü: {len(df)} satır")
            return df

        except Exception as e:
            logger.error(f"Veri dönüştürme hatası: {e}")
            raise

    def _apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """İş kurallarını uygular."""
        # Belirli depolar için kalan siparişi sıfırla
        mask = df['DEPO'].isin(self.ZERO_ORDER_DEPOTS)
        df.loc[mask, 'Kalan Siparis'] = 0

        zero_count = mask.sum()
        if zero_count > 0:
            logger.info(f"{zero_count} satır için kalan sipariş sıfırlandı (DEPO: {self.ZERO_ORDER_DEPOTS})")

        return df

    def _normalize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Veri tiplerini normalleştirir."""
        try:
            # Tarih sütununu düzenle
            if 'Tarih' in df.columns:
                df['Tarih'] = pd.to_datetime(df['Tarih'], errors='coerce')
                df['Tarih'] = df['Tarih'].dt.strftime('%Y-%m-%d')
                df['Tarih'] = df['Tarih'].fillna('')

            # Sayısal sütunları düzenle
            numeric_columns = ['Kalan Siparis', 'Toplam Stok']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

            # Cari Kodu - SQL'den gelen halini koru
            if 'Cari Kodu' in df.columns:
                df['Cari Kodu'] = df['Cari Kodu'].astype(str).fillna('')
                logger.debug("Cari Kodu formatı korundu (SQL'den gelen orijinal format)")

            # Diğer string sütunları düzenle
            string_columns = ['DEPO', 'Malzeme Kodu', 'Sipariş No']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).fillna('')

            # Tüm NaN değerleri boş string yap
            df = df.fillna('')

            logger.debug("Veri tipleri normalleştirildi")
            return df

        except Exception as e:
            logger.error(f"Veri tipi normalleştirme hatası: {e}")
            raise

# ============================================================================
# SEVKIYAT ANALYZER
# ============================================================================

class SevkiyatAnalyzer:
    """Ana sevkiyat analiz sınıfı."""

    def __init__(self, config: SevkiyatConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.database)
        self.sheets_manager = GoogleSheetsManager(config.google_sheets)
        self.data_processor = SevkiyatDataProcessor(self.db_manager, config.config_manager)

    def run_analysis(self) -> None:
        """Tam analiz sürecini çalıştırır."""
        try:
            logger.info("Sevkiyat analizi başlatılıyor")

            # 1. Ham veriyi çek
            raw_data = self.data_processor.extract_raw_data()
            if raw_data.empty:
                logger.warning("Analiz edilecek veri bulunamadı")
                return

            # 2. Sevkiyat verisini dönüştür
            processed_data = self.data_processor.transform_data(raw_data)
            if processed_data.empty:
                logger.warning("Dönüştürülmüş sevkiyat verisi boş")
                return

            # 3. Cari verilerini çıkar
            cari_data = self.data_processor.extract_cari_data(processed_data)

            # 4. Borç verilerini oluştur
            borc_data = self.data_processor.create_borc_data(processed_data)

            # 5. Malzeme stok verilerini çek
            malzeme_data = self.data_processor._get_malzeme_data()

            # 6. Bekleyenler verilerini oluştur
            bekleyenler_data = self.data_processor.create_bekleyenler_data()

            # 7. Plan verilerini oluştur
            plan_data = self.data_processor.create_plan_data()

            # 8. Google Sheets'e yükle - Sevkiyat sayfası
            self.sheets_manager.update_worksheet(processed_data, 'Sevkiyat')

            # 9. Google Sheets'e yükle - Cari sayfası
            if not cari_data.empty:
                self.sheets_manager.update_worksheet(cari_data, 'Cari')
                logger.info(f"Cari verileri yüklendi: {len(cari_data)} kayıt")
            else:
                logger.warning("Cari verileri boş, yüklenmedi")

            # 10. Google Sheets'e yükle - Borc sayfası
            if not borc_data.empty:
                self.sheets_manager.update_worksheet(borc_data, 'Borc')
                logger.info(f"Borç verileri yüklendi: {len(borc_data)} kayıt")
            else:
                logger.warning("Borç verileri boş, yüklenmedi")

            # 11. Google Sheets'e yükle - Malzeme sayfası
            if not malzeme_data.empty:
                self.sheets_manager.update_worksheet(malzeme_data, 'Malzeme')
                logger.info(f"Malzeme verileri yüklendi: {len(malzeme_data)} kayıt")
            else:
                logger.warning("Malzeme verileri boş, yüklenmedi")

            # 12. Google Sheets'e yükle - Bekleyenler sayfası
            if not bekleyenler_data.empty:
                self.sheets_manager.update_worksheet(bekleyenler_data, 'Bekleyenler')
                logger.info(f"Bekleyenler verileri yüklendi: {len(bekleyenler_data)} kayıt")
            else:
                logger.warning("Bekleyenler verileri boş, yüklenmedi")

            # 13. Google Sheets'e yükle - Plan sayfası (boşsa da temizle)
            if not plan_data.empty:
                self.sheets_manager.update_worksheet(plan_data, 'Plan')
                logger.info(f"Plan verileri yüklendi: {len(plan_data)} kayıt")
            else:
                logger.warning("Plan verileri boş, worksheet temizleniyor")
                self.sheets_manager.update_worksheet(plan_data, 'Plan', clear_if_empty=True)

            logger.info(
                f"Sevkiyat analizi başarıyla tamamlandı: "
                f"{len(processed_data)} sevkiyat satırı, {len(cari_data)} cari kaydı, "
                f"{len(borc_data)} borç kaydı, {len(malzeme_data)} malzeme kaydı, "
                f"{len(bekleyenler_data)} bekleyenler kaydı, {len(plan_data)} plan kaydı işlendi"
            )

        except Exception as e:
            logger.error(f"Sevkiyat analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """Ana uygulama fonksiyonu."""
    try:
        logger.info("Sevkiyat analiz uygulaması başlatılıyor")

        # Konfigürasyonu yükle (Service Account otomatik başlar)
        config = SevkiyatConfig()
        logger.info("Konfigürasyon yüklendi")

        # Analizörü oluştur ve çalıştır
        analyzer = SevkiyatAnalyzer(config)
        analyzer.run_analysis()

        logger.info("Uygulama başarıyla tamamlandı")

    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından durduruldu")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
