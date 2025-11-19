"""
SAP Kodu Oluşturma Sistemi - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli SAP kodu analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- Tüm hassas bilgiler ve dosya yolları PRGsheet'te saklanır
- Sessiz çalışma (sadece log dosyasına yazar)
- ID > 8000 olan stok verilerini çeker ve 270'şer satırlık dosyalara böler
"""

import sys
import os

# Parent directory'yi Python path'e ekle (central_config için)
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import pyodbc
import logging
import pandas as pd
from contextlib import contextmanager
from pathlib import Path

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Log dosyasina yaz (konsol yok)
# PyInstaller ile freeze edildiginde dosya yollarini duzelt
if getattr(sys, 'frozen', False):
    base_dir = Path(sys.executable).parent
else:
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'sap_kodu_olustur.log'

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

class SAPKoduConfig:
    """
    Service Account ve merkezi config ile yapılandırma

    Artık:
    - SQL credentials kodda YOK
    - Dosya yolları environment variable'da YOK
    - Ayarlar PRGsheets'ten çekiliyor
    """

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()

            # PRGsheets'ten ayarları yükle
            self.settings = self.config_manager.get_settings()

            logger.info("Config yüklendi")

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            raise

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

    @property
    def sap_output_dir(self) -> str:
        """SAP çıktı dizini"""
        path = self.settings.get('SAP_OUTPUT_DIR')
        if not path:
            raise ValueError(
                "PRGsheet -> Ayar sayfasında SAP_OUTPUT_DIR ayarı eksik!\n"
                "Lütfen bu ayarı Global olarak ekleyin.\n"
                "Örnek değer: D:\\GoogleDrive\\Fiyat\\SAP"
            )
        return path

    @property
    def sap_chunk_size(self) -> int:
        """Her dosyada kaç satır olacak (varsayılan: 270)"""
        chunk_size = self.settings.get('SAP_CHUNK_SIZE', '270')
        return int(chunk_size)

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
            logger.info("Database connection established")
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")

# ============================================================================
# SAP KODU ANALYZER
# ============================================================================

class SAPKoduAnalyzer:
    """SAP Kodu stok verilerini analiz eden ana sınıf"""

    def __init__(self, config: SAPKoduConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)

    def get_sap_data(self) -> pd.DataFrame:
        """
        ID'si 8000'den büyük olan stok verilerini çeker

        Returns:
            Stok verileri DataFrame
        """
        query = """
        SELECT TOP 100 PERCENT
            sto_RECno AS [ID],
            sto_isim AS [MALZEME ADI],
            sto_kod AS [MALZEME KODU],
            dbo.fn_DepodakiMiktar(sto_kod,100,GetDate()) as DEPO,
            dbo.fn_DepodakiMiktar(sto_kod,300,GetDate()) as EXCLUSIVE,
            dbo.fn_DepodakiMiktar(sto_kod,200,GetDate()) as SUBE,
            dbo.fn_EldekiMiktar(sto_kod) AS [MIKTAR]
        FROM dbo.STOKLAR WITH (NOLOCK)
        WHERE (sto_pasif_fl IS NULL OR sto_pasif_fl=0)
            AND sto_RECno > 8000
        ORDER BY sto_kod
        """

        try:
            with self.db_manager.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(query)

                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                df = pd.DataFrame.from_records(rows, columns=columns)
                logger.info(f"ID > 8000 olan {len(df)} kayıt getirildi")

                return df

        except pyodbc.Error as e:
            logger.error(f"SAP sorgusu çalıştırma hatası: {e}")
            raise

    def process_sap_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        SAP verilerini işler ve filtreler

        Args:
            df: Ham stok verileri

        Returns:
            İşlenmiş ve filtrelenmiş DataFrame
        """
        if df.empty:
            return df

        # MALZEME KODU'nun ilk 10 hanesini al
        df['MALZEME_KODU_SHORT'] = df['MALZEME KODU'].astype(str).str[:10]

        # Tekrar edenleri çıkar - her kod'dan sadece bir tane
        df_unique = df.drop_duplicates(subset=['MALZEME_KODU_SHORT'], keep='first')

        # Sadece gerekli sütunları al ve yeniden yapılandır
        result_df = pd.DataFrame()
        result_df['MALZEME KODU'] = df_unique['MALZEME_KODU_SHORT']
        result_df['MIKTAR'] = 1  # Her kod için 1 değeri
        result_df['MALZEME ADI'] = df_unique['MALZEME ADI']

        # MALZEME ADI'na göre küçükten büyüğe sırala
        result_df = result_df.sort_values(by='MALZEME ADI', ascending=True)
        result_df = result_df.reset_index(drop=True)

        logger.info(f"Veri işleme tamamlandı: {len(result_df)} benzersiz kayıt")

        return result_df

    def create_output_directory(self) -> str:
        """
        Çıktı klasörünü oluşturur

        Returns:
            Çıktı dizini yolu
        """
        output_dir = self.config.sap_output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Çıktı klasörü oluşturuldu: {output_dir}")
        return output_dir

    def save_split_files(self, df: pd.DataFrame, output_dir: str) -> None:
        """
        Verileri chunk_size'a göre dosyalara böler ve kaydeder

        Args:
            df: İşlenmiş DataFrame
            output_dir: Çıktı dizini
        """
        if df.empty:
            logger.warning("Kaydedilecek veri yok (DataFrame boş)")
            return

        chunk_size = self.config.sap_chunk_size
        total_rows = len(df)

        logger.info(f"Toplam {total_rows} satır, {chunk_size}'şer satırlık dosyalara bölünüyor")

        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i+chunk_size].copy()
            file_number = (i // chunk_size) + 1
            filename = f"sap{file_number}.xlsx"
            filepath = os.path.join(output_dir, filename)

            # Excel'e kaydetmeden önce MALZEME KODU'nu sayıya çevirmeye çalış
            try:
                chunk['MALZEME KODU'] = pd.to_numeric(chunk['MALZEME KODU'])
            except (ValueError, TypeError):
                pass

            # ExcelWriter ile formatlama kontrolü
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                chunk.to_excel(writer, index=False, header=False, sheet_name='Sheet1')

                # Worksheet'i al ve MALZEME KODU sütununu "Genel" formatına ayarla
                worksheet = writer.sheets['Sheet1']
                for row in range(1, len(chunk) + 1):
                    cell = worksheet.cell(row=row, column=1)  # İlk sütun (MALZEME KODU)
                    cell.number_format = 'General'

            logger.info(f"{filename} dosyası kaydedildi - {len(chunk)} satır")

    def run_analysis(self) -> None:
        """Ana SAP kodu analizi workflow'u çalıştır"""
        try:
            logger.info("SAP Kodu analizi başlatılıyor...")

            # 1. Ham verileri çek (SQL'den)
            df = self.get_sap_data()

            if df.empty:
                logger.warning("ID > 8000 olan veri bulunamadı")
                return

            # 2. Verileri işle ve filtrele
            processed_df = self.process_sap_data(df)

            if processed_df.empty:
                logger.warning("İşlenmiş veri boş")
                return

            # 3. Çıktı klasörünü oluştur
            output_dir = self.create_output_directory()

            # 4. Dosyaları chunk_size'a göre böl ve kaydet
            self.save_split_files(processed_df, output_dir)

            logger.info("[OK] SAP Kodu analizi tamamlandı!")

        except Exception as e:
            logger.error(f"[HATA] SAP Kodu analizi hatası: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_sap_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = SAPKoduConfig()

        # Analyzer oluştur
        analyzer = SAPKoduAnalyzer(config)

        # Analiz çalıştır
        analyzer.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_sap_analysis()
