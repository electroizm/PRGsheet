import sys
import os

# Parent directory'yi Python path'e ekle (central_config icin)
# Bu script Service Account klasorunde oldugu icin, kendi klasorunu ekle
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

"""
Ciro Hesaplama Sistemi - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli ciro hesaplama

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- Tüm hassas bilgiler ve dosya yolları PRGsheet'te saklanır
- Sessiz çalışma (sadece log dosyasına yazar)
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

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
log_file = log_dir / 'ciro.log'

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

class CiroConfig:
    """
    Service Account ve merkezi config ile yapılandırma

    Artık:
    - Service account credentials kodda YOK
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
    def ciro_txt_dosya_yolu(self) -> str:
        """Ciro.txt dosya yolu"""
        path = self.settings.get('CIRO_TXT_DOSYA_YOLU')
        if not path:
            raise ValueError(
                "PRGsheet -> Ayar sayfasında CIRO_TXT_DOSYA_YOLU ayarı eksik!\n"
                "Lütfen bu ayarı Global olarak ekleyin.\n"
                "Örnek değer: D:/Dropbox/Ciro/Ciro.txt"
            )
        return path

    @property
    def merkez_sube_kodu(self) -> int:
        """Merkez şube kodu"""
        kod = self.settings.get('MERKEZ_SUBE_KODU')
        if not kod:
            raise ValueError(
                "PRGsheet -> Ayar sayfasında MERKEZ_SUBE_KODU ayarı eksik!\n"
                "Lütfen bu ayarı Global olarak ekleyin.\n"
                "Örnek değer: 1600704"
            )
        return int(kod)

    @property
    def sube_sube_kodu(self) -> int:
        """Şube şube kodu"""
        kod = self.settings.get('SUBE_SUBE_KODU')
        if not kod:
            raise ValueError(
                "PRGsheet -> Ayar sayfasında SUBE_SUBE_KODU ayarı eksik!\n"
                "Lütfen bu ayarı Global olarak ekleyin.\n"
                "Örnek değer: 1601175"
            )
        return int(kod)

# ============================================================================
# AY ÇEVİRİ
# ============================================================================

AY_CEVIRI = {
    "January": "Ocak",
    "February": "Şubat",
    "March": "Mart",
    "April": "Nisan",
    "May": "Mayıs",
    "June": "Haziran",
    "July": "Temmuz",
    "August": "Ağustos",
    "September": "Eylül",
    "October": "Ekim",
    "November": "Kasım",
    "December": "Aralık"
}

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yöneticisi

    Artık:
    - Service account YOK
    - Service Account token kullanılıyor
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def get_siparis_data(self) -> pd.DataFrame:
        """
        PRGsheets → Siparis sayfasından verileri çek

        Returns:
            Sipariş verileri DataFrame
        """
        try:
            # PRGsheet'ten Siparis verilerini çek
            data = self.config_manager.get_worksheet_data('PRGsheet', 'Siparis')

            if data:
                headers = data[0]
                rows = data[1:]
                df = pd.DataFrame(rows, columns=headers)
                logger.info(f"{len(df)} satır Siparis verisi yüklendi")
                return df

            logger.warning("Siparis verisi bulunamadı")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Siparis worksheet hatası: {e}")
            raise

# ============================================================================
# CIRO ANALYZER
# ============================================================================

class CiroAnalyzer:
    """Ciro hesaplama ana sınıfı"""

    def __init__(self, config: CiroConfig):
        self.config = config
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def hesapla_ciro_aylik(self, dataframe: pd.DataFrame, tarih_sutunu: str) -> Tuple[str, str]:
        """
        Aylık ciro hesapla

        Args:
            dataframe: Sipariş verileri
            tarih_sutunu: Tarih sütunu adı

        Returns:
            (ciro_formatli, tarih_turkce) tuple
        """
        if dataframe.empty:
            return "0K", ""

        try:
            # Manuel olarak ay-yıl gruplama yapalım
            df_temp = dataframe.copy()
            df_temp['ay_yil'] = df_temp[tarih_sutunu].dt.to_period('M')
            aylik_ciro = df_temp.groupby('ay_yil')["Tutar"].sum() / 1.10

            if not aylik_ciro.empty:
                son_ay_ciro = aylik_ciro.iloc[-1]
                son_ay_period = aylik_ciro.index[-1]
                son_ay_tarih_ingilizce = son_ay_period.strftime('%Y %B')
                son_ay_yil, son_ay_adi_ingilizce = son_ay_tarih_ingilizce.split()
                son_ay_adi_turkce = AY_CEVIRI.get(son_ay_adi_ingilizce, son_ay_adi_ingilizce)
                son_ay_tarih_turkce = f"{son_ay_yil} {son_ay_adi_turkce}"

                son_ay_ciro_yuvarlanmis_k = round(son_ay_ciro / 1000)
                son_ay_ciro_formatli = f"{son_ay_ciro_yuvarlanmis_k}K"

                return son_ay_ciro_formatli, son_ay_tarih_turkce
            else:
                return "0K", ""

        except Exception as e:
            logger.error(f"Ciro hesaplama hatası: {e}")
            return "0K", ""

    def ciro_bilgisini_yaz(
        self,
        dosya_yolu: str,
        merkez_tarih: str,
        merkez_ciro: str,
        sube_tarih: str,
        sube_ciro: str
    ) -> None:
        """
        Ciro bilgisini dosyaya yaz

        Args:
            dosya_yolu: Ciro.txt dosya yolu
            merkez_tarih: Merkez tarih bilgisi
            merkez_ciro: Merkez ciro bilgisi
            sube_tarih: Şube tarih bilgisi
            sube_ciro: Şube ciro bilgisi
        """
        try:
            with open(dosya_yolu, 'w', encoding='utf-8') as dosya:
                dosya.write(f"{merkez_tarih} Merkez cirosu: {merkez_ciro} ₺\n")
                dosya.write(f"{sube_tarih} Şube cirosu: {sube_ciro} ₺")

            logger.info(f"Ciro bilgisi yazıldı: {dosya_yolu}")

        except Exception as e:
            logger.error(f"Dosya yazma hatası: {e}")
            raise

    def run_analysis(self) -> None:
        """Ana ciro hesaplama workflow'u çalıştır"""
        try:
            logger.info("Ciro hesaplama başlatılıyor...")

            # 1. Sipariş verilerini çek (PRGsheets'ten - Service Account ile)
            df = self.sheets_manager.get_siparis_data()

            if df.empty:
                logger.error("Sipariş verisi bulunamadı!")
                raise ValueError("Sipariş verisi bulunamadı")

            # 2. Veri işleme
            # Virgüllü ondalık değerleri düzgün çevir
            df['Birim Fiyat'] = df['Birim Fiyat'].astype(str).str.replace(',', '.', regex=False)
            df['Birim Fiyat'] = pd.to_numeric(df['Birim Fiyat'], errors='coerce')

            df['Vergi'] = df['Vergi'].astype(str).str.replace(',', '.', regex=False)
            df['Vergi'] = pd.to_numeric(df['Vergi'], errors='coerce')

            df['Miktar'] = pd.to_numeric(df['Miktar'], errors='coerce')
            df['Mağaza'] = pd.to_numeric(df['Mağaza'], errors='coerce')
            df['Tutar'] = df['Birim Fiyat'] * df['Miktar'] + df['Vergi']

            tarih_sutunu = "Tarih"
            df = df.copy()
            df[tarih_sutunu] = pd.to_datetime(df[tarih_sutunu], errors='coerce')

            # 3. Merkez ve şube verilerini ayır
            merkez_df = df[df["Mağaza"] == self.config.merkez_sube_kodu].copy()
            sube_df = df[df["Mağaza"] == self.config.sube_sube_kodu].copy()

            logger.info(f"Merkez satır sayısı: {len(merkez_df)}")
            logger.info(f"Şube satır sayısı: {len(sube_df)}")

            # 4. Ciro hesapla
            merkez_ciro, merkez_tarih = self.hesapla_ciro_aylik(merkez_df, tarih_sutunu)
            sube_ciro, sube_tarih = self.hesapla_ciro_aylik(sube_df, tarih_sutunu)

            logger.info(f"Merkez ciro: {merkez_ciro} ({merkez_tarih})")
            logger.info(f"Şube ciro: {sube_ciro} ({sube_tarih})")

            # 5. Ciro bilgisini dosyaya yaz
            self.ciro_bilgisini_yaz(
                self.config.ciro_txt_dosya_yolu,
                merkez_tarih,
                merkez_ciro,
                sube_tarih,
                sube_ciro
            )

            logger.info("[OK] Ciro hesaplama tamamlandı!")

        except Exception as e:
            logger.error(f"[HATA] Ciro hesaplama hatası: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_ciro_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = CiroConfig()

        # Analyzer oluştur
        analyzer = CiroAnalyzer(config)

        # Analiz çalıştır
        analyzer.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_ciro_analysis()
