"""
Siparis Analysis System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli sipariş analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
- Batch processing ile performans optimizasyonu
"""

import pyodbc
import logging
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd
from contextlib import contextmanager
import time
from pathlib import Path
import sys

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

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
log_file = log_dir / 'siparis_analizi.log'

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

class SiparisConfig:
    """
    Sipariş analizi için yapılandırma sınıfı
    Service Account ve merkezi config kullanır
    """

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
    def spreadsheet_id(self) -> str:
        """PRGsheet spreadsheet ID"""
        return self.config_manager.MASTER_SPREADSHEET_ID

    @property
    def connection_string(self) -> str:
        """
        SQL bağlantı string'i
        Tüm ayarlar PRGsheet -> Ayar'dan çekilir
        """
        # Gerekli ayarları kontrol et
        required_settings = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
        missing = [key for key in required_settings if not self.settings.get(key)]

        if missing:
            raise ValueError(
                f"PRGsheet -> Ayar sayfasında eksik ayarlar: {', '.join(missing)}\n"
                f"Lütfen bu ayarları Global olarak ekleyin."
            )

        sql_server = self.settings['SQL_SERVER']
        sql_database = self.settings['SQL_DATABASE']
        sql_username = self.settings['SQL_USERNAME']
        sql_password = self.settings['SQL_PASSWORD']

        return (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={sql_server};'
            f'DATABASE={sql_database};'
            f'UID={sql_username};'
            f'PWD={sql_password};'
            f'TrustServerCertificate=yes;'
            f'Connection Timeout=30;'
        )

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Veritabanı bağlantı yöneticisi"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.max_retries = 3
        self.retry_delay = 2

    @contextmanager
    def get_connection(self):
        """Güvenli veritabanı bağlantısı context manager"""
        connection = None
        retry_count = 0

        try:
            while retry_count < self.max_retries:
                try:
                    connection = pyodbc.connect(self.connection_string)
                    yield connection
                    break

                except pyodbc.Error as e:
                    retry_count += 1
                    if retry_count >= self.max_retries:
                        logger.error(f"Veritabanı bağlantı hatası: {e}")
                        raise
                    time.sleep(self.retry_delay)

        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

    def execute_query(self, connection: pyodbc.Connection, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """Güvenli sorgu çalıştırıcı (SQL Injection korumalı)"""
        try:
            cursor = connection.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchall()

        except pyodbc.Error as e:
            logger.error(f"Sorgu çalıştırma hatası: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yöneticisi
    Service account yerine Service Account token kullanır
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client
        self.max_retries = 3
        self.retry_delay = 2

    def get_date_37_days_ago(self) -> str:
        """37 gün önceki tarihi YYYYMMDD formatında döndürür"""
        date_37_days_ago = datetime.now() - timedelta(days=37)
        return date_37_days_ago.strftime('%Y%m%d')

    def update_siparis_worksheet(self, data: pd.DataFrame) -> None:
        """Sipariş çalışma sayfasını günceller"""
        retry_count = 0

        while retry_count < self.max_retries:
            try:
                # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
                spreadsheet = self.gc.open_by_key(
                    self.config_manager.MASTER_SPREADSHEET_ID
                )

                try:
                    siparis_worksheet = spreadsheet.worksheet('Siparis')
                    siparis_worksheet.clear()
                except:
                    siparis_worksheet = spreadsheet.add_worksheet(title='Siparis', rows=2000, cols=25)

                if not data.empty:
                    # NaN değerleri boş string ile değiştir
                    data_cleaned = data.fillna('')

                    # "Cari Kod" sütununu string olarak formatla (bilimsel notasyon engelle)
                    if 'Cari Kod' in data_cleaned.columns:
                        data_cleaned['Cari Kod'] = data_cleaned['Cari Kod'].astype(str)

                    # Sütun başlıklarını ve verileri hazırla
                    values = [data_cleaned.columns.values.tolist()] + data_cleaned.values.tolist()

                    # Verileri toplu olarak güncelle (RAW: binlik ayraç eklenmez)
                    siparis_worksheet.update(values, value_input_option='RAW')

                break  # Başarılı olursa döngüden çık

            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retries:
                    logger.error(f"Google Sheets güncelleme başarısız: {e}")
                    raise
                time.sleep(self.retry_delay)

# ============================================================================
# SIPARIS ANALYZER
# ============================================================================

class SiparisAnalyzer:
    """Sipariş analiz sınıfı"""

    def __init__(self, config: SiparisConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def _get_cari_unvan_data(self, connection: pyodbc.Connection, cari_codes) -> pd.DataFrame:
        """Cari hesap unvan bilgilerini getirir (batch processing)"""
        try:
            cari_codes_list = cari_codes.tolist() if hasattr(cari_codes, 'tolist') else list(cari_codes)
            clean_codes = [str(code).strip() for code in cari_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

            # Batch işlem (1000'er parça)
            batch_size = 1000
            all_results = []

            for i in range(0, len(clean_codes), batch_size):
                batch_codes = clean_codes[i:i + batch_size]
                placeholders = ', '.join(['?' for _ in batch_codes])

                query = f"""
                SELECT [cari_kod] AS [Cari Kod],
                       ISNULL([cari_unvan1], '') AS [Cari Adi]
                FROM [dbo].[CARI_HESAPLAR]
                WHERE [cari_kod] IN ({placeholders})
                """

                cursor = connection.cursor()
                cursor.execute(query, batch_codes)
                rows = cursor.fetchall()

                if rows:
                    columns = [column[0] for column in cursor.description]
                    batch_df = pd.DataFrame.from_records(rows, columns=columns)
                    all_results.append(batch_df)

                cursor.close()

            if all_results:
                return pd.concat(all_results, ignore_index=True)
            else:
                return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

        except Exception as e:
            logger.error(f"Cari unvan veri hatası: {e}")
            return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

    def _get_personel_data(self, connection: pyodbc.Connection, satici_codes) -> pd.DataFrame:
        """Personel bilgilerini getirir"""
        try:
            satici_codes_list = satici_codes.tolist() if hasattr(satici_codes, 'tolist') else list(satici_codes)
            clean_codes = [str(code).strip() for code in satici_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['cari_per_kod', 'Personel'])

            placeholders = ', '.join(['?' for _ in clean_codes])
            query = f"""
            SELECT cari_per_kod,
                   ISNULL(cari_per_adi, '') + ' ' + ISNULL(cari_per_soyadi, '') AS [Personel]
            FROM [dbo].[CARI_PERSONEL_TANIMLARI]
            WHERE cari_per_kod IN ({placeholders})
            """

            cursor = connection.cursor()
            cursor.execute(query, clean_codes)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return pd.DataFrame.from_records(rows, columns=columns)

        except Exception as e:
            logger.error(f"Personel veri hatası: {e}")
            return pd.DataFrame(columns=['cari_per_kod', 'Personel'])

    def _get_stok_data(self, connection: pyodbc.Connection, stok_codes) -> pd.DataFrame:
        """Stok bilgilerini getirir (batch processing)"""
        try:
            stok_codes_list = stok_codes.tolist() if hasattr(stok_codes, 'tolist') else list(stok_codes)
            clean_codes = [str(code).strip() for code in stok_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

            # Batch işlem (1000'er parça)
            batch_size = 1000
            all_results = []

            for i in range(0, len(clean_codes), batch_size):
                batch_codes = clean_codes[i:i + batch_size]
                placeholders = ', '.join(['?' for _ in batch_codes])

                query = f"""
                SELECT [sto_kod],
                       ISNULL([sto_isim], '') AS [Malzeme Adı]
                FROM [dbo].[STOKLAR]
                WHERE [sto_kod] IN ({placeholders})
                """

                cursor = connection.cursor()
                cursor.execute(query, batch_codes)
                rows = cursor.fetchall()

                if rows:
                    columns = [column[0] for column in cursor.description]
                    batch_df = pd.DataFrame.from_records(rows, columns=columns)
                    all_results.append(batch_df)

                cursor.close()

            if all_results:
                return pd.concat(all_results, ignore_index=True)
            else:
                return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

        except Exception as e:
            logger.error(f"Stok veri hatası: {e}")
            return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

    def get_siparis_data(self, start_date: str) -> pd.DataFrame:
        """Belirtilen tarihten sonraki sipariş verilerini getirir"""
        query = """
        SELECT [sip_tarih] AS [Tarih],
               [sip_evrakno_sira] AS [Sipariş No],
               [sip_satirno] AS [Satir],
               [sip_belgeno] AS [Sozlesme],
               [sip_satici_kod] AS [Satici],
               [sip_musteri_kod] AS [Cari Kod],
               [sip_stok_kod] AS [Malzeme Kodu],
               [sip_b_fiyat] AS [Birim Fiyat],
               [sip_miktar] AS [Miktar],
               [sip_teslim_miktar] AS [Teslimat],
               [sip_iskonto_1] AS [Iskonto],
               [sip_vergi_pntr] AS [VergiKod],
               [sip_vergi] AS [Vergi],
               [sip_aciklama] AS [Aciklama],
               [sip_aciklama2] AS [Header],
               [sip_depono] AS [Depo],
               [sip_cari_sormerk] AS [Mağaza]
        FROM [dbo].[SIPARISLER]
        WHERE [sip_tarih] >= ? AND [sip_evrakno_seri] != 'S'
        ORDER BY [sip_evrakno_sira] DESC
        """

        with self.db_manager.get_connection() as connection:
            try:
                # Ana sipariş verilerini getir
                cursor = connection.cursor()
                cursor.execute(query, (start_date,))
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                df = pd.DataFrame.from_records(rows, columns=columns)

                if df.empty:
                    return df

                # Tarih formatını düzenle
                if 'Tarih' in df.columns:
                    df['Tarih'] = pd.to_datetime(df['Tarih'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['Tarih'] = df['Tarih'].fillna('')

                # Cari hesap bilgilerini ekle
                if 'Cari Kod' in df.columns:
                    unique_cari_codes = df['Cari Kod'].dropna().unique()
                    if len(unique_cari_codes) > 0:
                        cari_df = self._get_cari_unvan_data(connection, unique_cari_codes)
                        df = pd.merge(df, cari_df, on='Cari Kod', how='left')

                # Personel bilgilerini ekle
                if 'Satici' in df.columns:
                    unique_satici_codes = df['Satici'].dropna().unique()
                    if len(unique_satici_codes) > 0:
                        personel_df = self._get_personel_data(connection, unique_satici_codes)
                        df = pd.merge(df, personel_df, left_on='Satici', right_on='cari_per_kod', how='left')

                        # Gereksiz sütunları kaldır
                        columns_to_drop = ['cari_per_kod', 'Satici']
                        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

                # Stok bilgilerini ekle
                if 'Malzeme Kodu' in df.columns:
                    unique_stok_codes = df['Malzeme Kodu'].dropna().unique()
                    if len(unique_stok_codes) > 0:
                        stok_df = self._get_stok_data(connection, unique_stok_codes)
                        df = pd.merge(df, stok_df, left_on='Malzeme Kodu', right_on='sto_kod', how='left')

                        if 'sto_kod' in df.columns:
                            df = df.drop(columns=['sto_kod'])

                # Sütun sıralamasını yeniden düzenle
                desired_columns = [
                    'Satir', 'Sipariş No', 'Tarih', 'Sozlesme', 'Cari Kod', 'Cari Adi',
                    'Malzeme Kodu', 'Malzeme Adı', 'Miktar', 'Teslimat', 'Depo',
                    'Personel', 'Mağaza', 'Birim Fiyat', 'Vergi', 'Iskonto', 'Header', 'Aciklama'
                ]

                available_columns = [col for col in desired_columns if col in df.columns]
                remaining_columns = [col for col in df.columns if col not in available_columns]
                final_columns = available_columns + remaining_columns

                df = df[final_columns]

                # Sipariş No büyükten küçüğe, Satir küçükten büyüğe sırala
                if 'Sipariş No' in df.columns and 'Satir' in df.columns:
                    df = df.sort_values(by=['Sipariş No', 'Satir'], ascending=[False, True], na_position='last')
                    df = df.reset_index(drop=True)

                return df

            except Exception as e:
                logger.error(f"Sipariş veri hatası: {e}")
                raise

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # 37 gün önceki tarihi hesapla
            start_date = self.sheets_manager.get_date_37_days_ago()

            # Sipariş verilerini getir
            siparis_df = self.get_siparis_data(start_date)

            if not siparis_df.empty:
                # Google Sheets'e yükle
                self.sheets_manager.update_siparis_worksheet(siparis_df)

        except Exception as e:
            logger.error(f"Sipariş analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_siparis_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = SiparisConfig()

        # Analyzer oluştur
        analyzer = SiparisAnalyzer(config)

        # Analiz çalıştır
        analyzer.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_siparis_analysis()
