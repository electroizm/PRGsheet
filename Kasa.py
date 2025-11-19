"""
Kasa System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli kasa analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pyodbc
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd
from contextlib import contextmanager

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
log_file = log_dir / 'kasa_analizi.log'

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

class KasaConfig:
    """
    Kasa analizi için yapılandırma sınıfı
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

    def execute_query(self, query: str) -> pd.DataFrame:
        """SQL sorgusu çalıştırma"""
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
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
    Service account yerine Service Account token kullanır
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame'i Google Sheets için temizle"""
        df_copy = df.copy()

        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].astype(str)
            elif df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(
                    lambda x: str(x) if hasattr(x, 'strftime') else x
                )

        return df_copy.fillna('').replace([pd.NA, float('inf'), float('-inf')], '')

    def save_to_worksheet(self, df: pd.DataFrame, worksheet_name: str) -> None:
        """Kasa sayfasına veri yaz"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            # Worksheet'i bul veya oluştur
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name,
                    rows=1000,
                    cols=10
                )

            if not df.empty:
                # Clean data
                df_clean = self._clean_dataframe(df)
                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

                # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
                worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            logger.error(f"Kasa worksheet güncelleme hatası: {e}")
            raise

# ============================================================================
# KASA PROCESSOR
# ============================================================================

class KasaDataProcessor:
    """Kasa veri işleyici"""

    KASA_COLUMN_MAPPING = {
        "#msg_S_0200": "KASA KODU",
        "#msg_S_0201": "KASA ADI",
        "msg_S_0089": "Tarih",
        "#msg_S_0085": "ACIKLAMA",
        "msg_S_0100": "Alacak / Borç",
        "#msg_S_0103\\T": "TUTAR",
        "msg_S_0115": "CARI ADI",
        "msg_S_0094": "Evrak Tipi",
        "msg_S_0003": "Nakit / Dekont"
    }

    KASA_COLUMNS = [
        "Tarih", "KASA KODU", "KASA ADI", "CARI ADI",
        "TUTAR", "ACIKLAMA", "Alacak / Borç", "Nakit / Dekont"
    ]

    def __init__(self, config: KasaConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def process_kasa_data(self, days_filter: int = 377) -> pd.DataFrame:
        """Kasa verilerini işle"""
        query = """
        SELECT * FROM dbo.fn_CariFoy (
            N'0',4,N'',NULL,'20241231','20240101','20771231',0,N''
        ) ORDER BY [msg_S_0088] DESC
        """

        df = self.db_manager.execute_query(query)
        df = df.rename(columns=self.KASA_COLUMN_MAPPING)
        df = df[self.KASA_COLUMNS].copy()

        df["Tarih"] = pd.to_datetime(df["Tarih"], format="%Y-%m-%d")
        date_threshold = datetime.now() - timedelta(days=days_filter)
        df = df[df["Tarih"] > date_threshold]

        df["Tarih"] = df["Tarih"].dt.strftime("%Y-%m-%d")

        return df

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # Kasa verilerini işle
            kasa_df = self.process_kasa_data()

            # Google Sheets'e kaydet
            if not kasa_df.empty:
                self.sheets_manager.save_to_worksheet(kasa_df, "Kasa")

        except Exception as e:
            logger.error(f"Kasa analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_kasa_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = KasaConfig()

        # Processor oluştur
        processor = KasaDataProcessor(config)

        # Analiz çalıştır
        processor.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_kasa_analysis()
