"""
Tamamlanan System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli tamamlanan sipariş analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pyodbc
import logging
from datetime import datetime
from typing import Optional
from contextlib import contextmanager
import pandas as pd
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
log_file = log_dir / 'tamamlanan_analizi.log'

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

class TamamlananConfig:
    """
    Tamamlanan analizi için yapılandırma sınıfı
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
            f'PWD={sql_password}'
        )

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Veritabanı işlemleri yöneticisi"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @contextmanager
    def get_connection(self):
        """Context manager ile güvenli bağlantı yönetimi"""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string)
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Güvenli sorgu çalıştırma (SQL Injection korumalı)

        Args:
            query: SQL sorgusu (? placeholder'ları ile)
            params: Parametreler tuple'ı

        Returns:
            Sorgu sonuçları DataFrame
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                return pd.DataFrame.from_records(rows, columns=columns)

            except pyodbc.Error as e:
                logger.error(f"Query execution error: {e}")
                raise

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yöneticisi

    Artık:
    - Private key YOK
    - Service account YOK
    - Service Account token kullanılıyor
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def _clean_dataframe_for_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for JSON serialization"""
        df_clean = df.copy()
        df_clean = df_clean.fillna('')

        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                df_clean[col] = df_clean[col].astype(str).replace('NaT', '')
            elif df_clean[col].dtype in ['float64', 'float32']:
                df_clean[col] = df_clean[col].replace([float('inf'), float('-inf')], '')
                df_clean[col] = df_clean[col].astype(str).replace('nan', '').replace('NaN', '')
            elif df_clean[col].dtype in ['int64', 'int32']:
                df_clean[col] = df_clean[col].astype(str)
            else:
                df_clean[col] = df_clean[col].astype(str).replace('nan', '').replace('NaN', '').replace('None', '')

        # Final cleanup
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: '' if str(x) in ['nan', 'NaN', 'None', 'NaT'] else str(x)
            )

        return df_clean

    def save_to_worksheet(self, df: pd.DataFrame, worksheet_name: str) -> None:
        """Save DataFrame to Google Sheets worksheet"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)

            if not df.empty:
                # Clean data for JSON serialization
                df_clean = self._clean_dataframe_for_json(df)

                # "sip_musteri_kod" sütununu string olarak formatla (bilimsel notasyon engelle)
                if 'sip_musteri_kod' in df_clean.columns:
                    df_clean['sip_musteri_kod'] = df_clean['sip_musteri_kod'].astype(str)

                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
                # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
                worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            logger.error(f"{worksheet_name} worksheet güncelleme hatası: {e}")
            raise

    def _get_sip_tarih_from_ayar(self) -> str:
        """
        Ayar sayfasından sip_tarih değerini oku

        Returns:
            str: sip_tarih değeri (örn: '2023-09-01')
        """
        try:
            spreadsheet = self.gc.open_by_key(self.config_manager.MASTER_SPREADSHEET_ID)
            sheet = spreadsheet.worksheet('Ayar')
            config = {row[0]: row[1] for row in sheet.get_all_values() if len(row) >= 2}

            # Kayıtlı sip_tarih değerini kullan
            if 'sip_tarih' in config and config['sip_tarih']:
                return config['sip_tarih']
            else:
                # Varsayılan olarak 2023-09-01
                return '2023-09-01'
        except Exception:
            # Hata durumunda varsayılan değer
            return '2023-09-01'

# ============================================================================
# TAMAMLANAN PROCESSOR
# ============================================================================

class TamamlananProcessor:
    """Tamamlanan sipariş işleyici"""

    def __init__(self, config: TamamlananConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def get_tamamlanan_data(self) -> pd.DataFrame:
        """Tamamlanan veri SQL'den çek"""
        # Ayar sayfasından sip_tarih değerini al
        sip_tarih = self.sheets_manager._get_sip_tarih_from_ayar()

        query = f"""
            SELECT TOP 100 PERCENT
                MIN(sip_RECno) AS [msg_S_0966], /* İLK KAYIT NO */
                sip_musteri_kod AS [msg_S_0200], /* CARİ KODU */
                sip_belgeno,
                sip_musteri_kod,
                sip_evrakno_sira AS [msg_S_0789], /* EVRAK SIRA */
                sip_tarih AS [msg_S_1072], /* HAREKET TARİHİ */
                SUM(CASE WHEN (sip_kapat_fl=1) THEN 0
                    ELSE sip_miktar - sip_teslim_miktar
                END) AS [msg_S_0247] /* KALAN MİKTAR */
            FROM dbo.SIPARISLER WITH (NOLOCK)
            WHERE sip_tarih > ?
            GROUP BY sip_tip, sip_musteri_kod, sip_tarih, sip_evrakno_sira, sip_belgeno
            HAVING SUM(CASE WHEN (sip_kapat_fl=1) THEN 0
                ELSE sip_miktar - sip_teslim_miktar
            END) = 0
            ORDER BY sip_tip, sip_musteri_kod, sip_tarih, sip_evrakno_sira, sip_belgeno
        """

        df = self.db_manager.execute_query(query, (sip_tarih,))

        # Select only required columns
        required_columns = ['sip_belgeno','sip_musteri_kod', 'msg_S_1072', 'msg_S_0789']
        return df[required_columns]

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # Veriyi yükle
            tamamlanan_data = self.get_tamamlanan_data()

            # Google Sheets'e kaydet - Tamamlanan sayfası
            if not tamamlanan_data.empty:
                self.sheets_manager.save_to_worksheet(tamamlanan_data, "Tamamlanan")

        except Exception as e:
            logger.error(f"Tamamlanan analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_tamamlanan_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = TamamlananConfig()

        # Processor oluştur
        processor = TamamlananProcessor(config)

        # Analiz çalıştır
        processor.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_tamamlanan_analysis()
