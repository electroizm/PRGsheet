"""
Montaj System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli montaj analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- Excel'den veri okuma ve Sheets'e yazma
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pandas as pd
import logging
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
log_file = log_dir / 'montaj_analizi.log'

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

class MontajConfig:
    """
    Montaj analizi için yapılandırma sınıfı
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
    def excel_dosya_yolu(self) -> str:
        """
        Excel dosya yolu
        PRGsheet -> Ayar'dan MONTAJ_EXCEL_PATH alınır
        """
        excel_path = self.settings.get('MONTAJ_EXCEL_PATH', '')

        if not excel_path:
            # Varsayılan yol
            excel_path = r"D:/GoogleDrive/PRG/Montaj Raporu.xlsx"

        return excel_path

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

    def clean_dataframe_for_json(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def read_montaj_from_sheets(self) -> pd.DataFrame:
        """PRGsheets'ten mevcut Montaj sayfasını okur"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            try:
                worksheet = spreadsheet.worksheet("Montaj")
                data = worksheet.get_all_values()

                if len(data) > 1:  # Başlık + veri varsa
                    df = pd.DataFrame(data[1:], columns=data[0])

                    # Sadece gerekli sütunları seç
                    df = df[['Servis Bakım ID', 'Sözleşme Numarası']].copy()

                    # Servis Bakım ID'yi integer'a çevir
                    if 'Servis Bakım ID' in df.columns and len(df) > 0:
                        df['Servis Bakım ID'] = pd.to_numeric(df['Servis Bakım ID'], errors='coerce')
                        df = df.dropna(subset=['Servis Bakım ID'])
                        if len(df) > 0:
                            df['Servis Bakım ID'] = df['Servis Bakım ID'].astype('int64')

                    # Sözleşme Numarasını integer'a çevir
                    if 'Sözleşme Numarası' in df.columns and len(df) > 0:
                        df['Sözleşme Numarası'] = pd.to_numeric(df['Sözleşme Numarası'], errors='coerce')
                        df = df.dropna(subset=['Sözleşme Numarası'])
                        if len(df) > 0:
                            df['Sözleşme Numarası'] = df['Sözleşme Numarası'].astype('int64')

                    return df
                else:
                    return pd.DataFrame(columns=['Servis Bakım ID', 'Sözleşme Numarası'])

            except:
                return pd.DataFrame(columns=['Servis Bakım ID', 'Sözleşme Numarası'])

        except Exception:
            return pd.DataFrame(columns=['Servis Bakım ID', 'Sözleşme Numarası'])

    def save_to_worksheet(self, df: pd.DataFrame) -> None:
        """DataFrame'i PRGsheets'in Montaj sayfasına kaydeder"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            try:
                worksheet = spreadsheet.worksheet("Montaj")
                worksheet.clear()
            except:
                worksheet = spreadsheet.add_worksheet(title="Montaj", rows=1000, cols=20)

            if not df.empty:
                # Clean data for JSON serialization
                df_clean = self.clean_dataframe_for_json(df)
                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
                worksheet.update(values, value_input_option='USER_ENTERED')

        except Exception as e:
            logger.error(f"Montaj sayfasına kayıt hatası: {e}")
            raise

# ============================================================================
# MONTAJ PROCESSOR
# ============================================================================

class MontajProcessor:
    """Montaj veri işleyici"""

    def __init__(self, config: MontajConfig):
        self.config = config
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def read_montaj_raporu(self, file_path: str) -> pd.DataFrame:
        """
        Montaj Raporu.xlsx dosyasından Servis Bakım ID ve Sözleşme Numarası sütunlarını okur

        Args:
            file_path: Montaj Raporu.xlsx dosyasının tam yolu

        Returns:
            Temizlenmiş ve gruplandırılmış DataFrame
        """
        try:
            # Excel dosyasını oku
            df = pd.read_excel(file_path)

            # Sadece gerekli sütunları seç
            df_filtered = df[['Servis Bakım ID', 'Sözleşme Numarası']].copy()

            # Sözleşme Numarası boş olan satırları sil
            df_filtered = df_filtered.dropna(subset=['Sözleşme Numarası'])

            # Sözleşme Numarasını integer'a çevir
            df_filtered['Sözleşme Numarası'] = df_filtered['Sözleşme Numarası'].astype('int64')

            # Servis Bakım ID'ye göre gruplandir (ilk değeri al)
            df_grouped = df_filtered.groupby('Servis Bakım ID', as_index=False).first()

            return df_grouped

        except Exception as e:
            logger.error(f"Excel dosyası okunurken hata: {e}")
            raise

    def merge_and_update_data(self, new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
        """
        Yeni veriyi mevcut veriyle birleştirir
        Aynı Servis Bakım ID varsa günceller, yoksa ekler
        """
        if existing_df.empty:
            return new_df

        # Index'leri sıfırla
        existing_df = existing_df.reset_index(drop=True)
        new_df = new_df.reset_index(drop=True)

        # Birleştir (Servis Bakım ID'ye göre)
        merged_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(
            subset=['Servis Bakım ID'],
            keep='last'  # Yeni veriyi tut
        )

        # Servis Bakım ID'ye göre sırala
        merged_df = merged_df.sort_values('Servis Bakım ID').reset_index(drop=True)

        return merged_df

    def run_update(self) -> None:
        """Ana işlem fonksiyonu"""
        try:
            # Montaj Raporu'nu oku
            montaj_file = self.config.excel_dosya_yolu
            new_data = self.read_montaj_raporu(montaj_file)

            # Mevcut Montaj sayfasını oku
            existing_data = self.sheets_manager.read_montaj_from_sheets()

            # Verileri birleştir (güncelle/ekle)
            final_data = self.merge_and_update_data(new_data, existing_data)

            # PRGsheets'e kaydet
            self.sheets_manager.save_to_worksheet(final_data)

        except Exception as e:
            logger.error(f"Montaj güncellemesi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_montaj_update() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = MontajConfig()

        # Processor oluştur
        processor = MontajProcessor(config)

        # Güncelleme çalıştır
        processor.run_update()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_montaj_update()
