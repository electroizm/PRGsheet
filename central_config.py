"""
Merkezi Google Service Account ve Config Yönetimi
Tüm PRG uygulamaları için ortak kullanım

Kullanılan Modüller:
    Risk, Kasa, Irsaliye, SanalPos, Ciro, Montaj, OKC, SSH,
    Siparis, Sevkiyat, Stok, BekleyenAPI, BagKodu, Bakiye,
    Fiyat_Mikro, SAP_Kodu_Olustur, Tamamlanan ve daha fazlası...

Kullanım Örnekleri:
    from central_config import CentralConfigManager

    # 1. Spreadsheet'e erişim
    manager = CentralConfigManager()
    risk_sheet = manager.get_spreadsheet('Risk')
    kasa_sheet = manager.get_spreadsheet('Kasa')

    # 2. Worksheet verisini okuma
    data = manager.get_worksheet_data('PRGsheet', 'NoRisk')

    # 3. Ayarları çekme (Global ve App-specific)
    settings = manager.get_settings()
    sql_server = settings.get('SQL_SERVER')
    etiket_url = settings.get('Etiket_ETIKET_BASLIK_URL')
"""

import gspread
from google.oauth2.service_account import Credentials
import os
import sys
from pathlib import Path
from typing import Dict, Optional, List
import logging
import json
from cryptography.fernet import Fernet

# Logging ayarları
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




# ============================================================================
# ŞİFRELİ CACHE YÖNETİMİ
# ============================================================================

class SettingsCache:
    """Ayarları şifreli olarak lokal cache'e kaydet/yükle"""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.key_file = os.path.join(base_dir, '.settings_key')
        self.cache_file = os.path.join(base_dir, '.settings_cache')
        self._init_encryption_key()

    def _init_encryption_key(self):
        if os.path.exists(self.key_file):
            try:
                with open(self.key_file, 'rb') as f:
                    self.key = f.read()
                self.cipher = Fernet(self.key)
                logger.info("Encryption key loaded")
            except:
                self._create_new_key()
        else:
            self._create_new_key()

    def _create_new_key(self):
        self.key = Fernet.generate_key()
        self.cipher = Fernet(self.key)
        try:
            with open(self.key_file, 'wb') as f:
                f.write(self.key)
        except Exception as e:
            logger.warning(f"Key save error: {e}")

    def save(self, settings: Dict[str, str]) -> bool:
        try:
            json_data = json.dumps(settings, ensure_ascii=False)
            encrypted = self.cipher.encrypt(json_data.encode('utf-8'))
            with open(self.cache_file, 'wb') as f:
                f.write(encrypted)
            logger.info(f"{len(settings)} settings cached")
            return True
        except Exception as e:
            logger.warning(f"Cache save error: {e}")
            return False

    def load(self) -> Optional[Dict[str, str]]:
        if not os.path.exists(self.cache_file):
            return None
        try:
            with open(self.cache_file, 'rb') as f:
                encrypted = f.read()
            decrypted = self.cipher.decrypt(encrypted)
            settings = json.loads(decrypted.decode('utf-8'))
            logger.info(f"{len(settings)} settings loaded from cache")
            return settings
        except:
            return None

    def clear(self):
        try:
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
        except:
            pass

class CentralConfigManager:
    """
    Merkezi Google Service Account ve Config Yönetimi

    Özellikler:
    - Service Account ile tüm Google Sheets'lere erişim
    - PRGsheet'ten config çekme
    - Spreadsheet ID yönetimi
    - SQL server ayarları yönetimi
    """

    # Ana config sayfası ID'si (PRGsheet)
    MASTER_SPREADSHEET_ID = '14Et1NH_yBrwymluEkL0_Ic7BCWev-FrCO-SuDVzkRPA'

    # Google Sheets API scope'ları
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    def __init__(self, service_account_file: str = None):
        """
        Initialize central config manager with Service Account

        Args:
            service_account_file: service_account.json dosya yolu (None ise otomatik bulur)
        """
        self.base_dir = self._get_base_dir()
        self.service_account_file = service_account_file or self._find_service_account_file()

        if not self.service_account_file:
            raise FileNotFoundError(
                f"service_account.json bulunamadı!\n"
                f"Lütfen service_account.json dosyasını şu konuma kopyalayın:\n"
                f"{self.base_dir}"
            )

        # Service Account ile Google Sheets client oluştur
        self.gc = self._authorize()

        # Config cache (bellekte)
        self.config_cache = {}
        self.settings_cache = {}

        # Şifreli lokal cache
        self.local_cache = SettingsCache(self.base_dir)

    def _get_base_dir(self) -> str:
        """Çalışma dizinini döndür (PyInstaller desteğiyle)"""
        if getattr(sys, 'frozen', False):
            # Exe olarak çalışıyorsa: exe'nin bulunduğu dizini kullan
            return os.path.dirname(sys.executable)
        else:
            # Normal Python olarak çalışıyorsa
            return os.path.dirname(os.path.abspath(__file__))

    def _find_service_account_file(self) -> Optional[str]:
        """service_account.json dosyasını bul (PyInstaller desteğiyle)"""
        possible_paths = [
            # PyInstaller'ın geçici extract klasörü (_MEIPASS)
            os.path.join(getattr(sys, '_MEIPASS', self.base_dir), 'service_account.json'),
            # Exe'nin bulunduğu dizin
            os.path.join(self.base_dir, 'service_account.json'),
            # Home dizini
            os.path.join(Path.home(), 'service_account.json'),
            # Varsayılan konum
            'D:/GoogleDrive/PRG/OAuth2/service_account.json',
        ]

        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Service Account dosyası bulundu: {path}")
                return path

        return None

    def _authorize(self) -> gspread.Client:
        """
        Google Service Account ile yetkilendirme

        Returns:
            gspread.Client: Yetkilendirilmiş Google Sheets client
        """
        try:
            # Service Account credentials oluştur
            creds = Credentials.from_service_account_file(
                self.service_account_file,
                scopes=self.SCOPES
            )

            # gspread client oluştur
            logger.info("Service Account ile yetkilendirme başarılı")
            return gspread.authorize(creds)

        except Exception as e:
            logger.error(f"Service Account yetkilendirme hatası: {e}")
            raise

    def load_spreadsheet_configs(self) -> Dict[str, str]:
        """
        PRGsheet → Config sayfasından tüm spreadsheet ID'lerini yükle
        
        """
        if self.config_cache:
            return self.config_cache

        try:
            # Ana config sayfasını aç
            master_sheet = self.gc.open_by_key(self.MASTER_SPREADSHEET_ID)
            config_worksheet = master_sheet.worksheet('Config')

            # Tüm config'leri oku
            configs = config_worksheet.get_all_records()

            # Dictionary'ye çevir (sadece aktif olanları)
            spreadsheet_map = {}
            for row in configs:
                app_name = str(row.get('App Name', '')).strip()
                spreadsheet_id = str(row.get('Spreadsheet ID', '')).strip()
                active = str(row.get('Active', 'TRUE')).strip().upper()

                if app_name and spreadsheet_id and active == 'TRUE':
                    spreadsheet_map[app_name] = spreadsheet_id

            self.config_cache = spreadsheet_map
            logger.info(f"{len(spreadsheet_map)} spreadsheet config yüklendi")
            return spreadsheet_map

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            return {}

    def get_spreadsheet(self, app_name: str) -> Optional[gspread.Spreadsheet]:
        """
        App adına göre spreadsheet'i getir

        Args:
            app_name: 'Risk', 'Etiket', 'Kasa', 'PRGsheet' gibi

        Returns:
            gspread.Spreadsheet nesnesi veya None

        Örnek:
            risk_sheet = manager.get_spreadsheet('Risk')
            worksheet = risk_sheet.worksheet('Risk')
        """
        # Config'leri yükle (cache yoksa)
        if not self.config_cache:
            self.load_spreadsheet_configs()

        # Spreadsheet ID'yi al
        spreadsheet_id = self.config_cache.get(app_name)

        if not spreadsheet_id:
            logger.error(f"'{app_name}' için spreadsheet ID bulunamadı!")
            logger.info(f"Mevcut app'ler: {list(self.config_cache.keys())}")
            return None

        # Spreadsheet'i aç
        try:
            spreadsheet = self.gc.open_by_key(spreadsheet_id)
            logger.info(f"'{app_name}' spreadsheet'i açıldı")
            return spreadsheet
        except Exception as e:
            logger.error(f"'{app_name}' spreadsheet açma hatası: {e}")
            return None

    def get_worksheet_data(
        self,
        app_name: str,
        worksheet_name: str
    ) -> List[List]:
        """
        Belirli bir app'in worksheet'inden veri çek

        Args:
            app_name: 'Risk', 'Etiket' gibi
            worksheet_name: 'NoRisk', 'Settings' gibi

        Returns:
            Worksheet verileri (2D liste)

        Örnek:
            data = manager.get_worksheet_data('PRGsheet', 'NoRisk')
            # [['Cari Kod'], ['120.01.001'], ['120.01.002'], ...]
        """
        spreadsheet = self.get_spreadsheet(app_name)

        if not spreadsheet:
            return []

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_values()
            logger.info(f"'{app_name}' → '{worksheet_name}': {len(data)} satır okundu")
            return data
        except Exception as e:
            logger.error(f"Worksheet okuma hatası ({app_name}/{worksheet_name}): {e}")
            return []

    def get_settings(self, use_cache: bool = True) -> Dict[str, str]:
        """
        PRGsheet → Settings/Ayar sayfasından TÜM ayarları yükle (IMPROVED VERSION)

        HIZLI LOKAL CACHE:
        - Ilk calistirmada: Google Sheets'ten ceker, lokal cache'e kaydeder
        - Sonraki calistirmalarda: Lokal cache'ten okur (HIZLI!)
        - Google Sheets degisirse: use_cache=False ile yenile

        Settings/Ayar sayfası formatı:
        | App Name | Key              | Value                  | Description      |
        |----------|------------------|------------------------|------------------|
        | Global   | SQL_SERVER       | 192.168.1.17           | SQL Server IP    |
        | Global   | SQL_DATABASE     | MikroDB_V14_DOGTAS_12  | Database adı     |
        | Etiket   | ETIKET_BASLIK_URL| https://drive...       | Etiket başlık    |

        Args:
            use_cache: True ise önce lokal cache'e bakar (varsayilan: True)

        Returns:
            {'SQL_SERVER': '192.168.1.17', 'Etiket_ETIKET_BASLIK_URL': 'https://...', ...}
            Not: App-specific settings are prefixed with 'AppName_'
        """
        # Bellekte cache varsa dondur
        if self.settings_cache:
            return self.settings_cache

        # Lokal cache'i dene (use_cache=True ise)
        if use_cache:
            cached_settings = self.local_cache.load()
            if cached_settings:
                self.settings_cache = cached_settings
                logger.info(f"FAST: {len(cached_settings)} settings loaded from local cache")
                return cached_settings

        # Google Sheets'ten cek
        try:
            logger.info("Fetching ALL settings from Google Sheets...")
            master_sheet = self.gc.open_by_key(self.MASTER_SPREADSHEET_ID)

            # Önce "Ayar" sayfasını dene, yoksa "Settings" dene
            try:
                settings_worksheet = master_sheet.worksheet('Ayar')
            except:
                settings_worksheet = master_sheet.worksheet('Settings')

            # Tüm ayarları oku
            settings = settings_worksheet.get_all_records()

            # TÜM ayarları yükle (Global ve App-specific)
            settings_dict = {}
            for row in settings:
                app_name = str(row.get('App Name', '')).strip()
                key = str(row.get('Key', '')).strip()
                value = str(row.get('Value', '')).strip()

                if not key:
                    continue

                # Global ayarlar: doğrudan key ile
                if app_name == 'Global' or app_name == '':
                    settings_dict[key] = value
                else:
                    # App-specific ayarlar: AppName_Key formatında
                    settings_dict[f"{app_name}_{key}"] = value

            # Bellekte cache'e kaydet
            self.settings_cache = settings_dict

            # Lokal cache'e sifreli kaydet
            self.local_cache.save(settings_dict)

            logger.info(f"{len(settings_dict)} settings loaded and cached (ALL from Ayar sheet)")
            return settings_dict

        except Exception as e:
            logger.warning(f"Settings yükleme hatası: {e}")
            return {}

    def get_app_settings(self, app_name: str) -> Dict[str, str]:
        """
        PRGsheet → Ayar sayfasından belirli bir app'in ayarlarını yükle

        Ayar sayfası formatı:
        | App Name | Key              | Value                  | Description      |
        |----------|------------------|------------------------|------------------|
        | Etiket   | ETIKET_BASLIK_URL| https://drive...       | Etiket başlık    |
        | Etiket   | YERLI_URETIM_URL | https://drive...       | Yerli üretim logo|

        Args:
            app_name: 'Etiket', 'Risk', 'Kasa' gibi uygulama adı

        Returns:
            {'ETIKET_BASLIK_URL': 'https://...', 'YERLI_URETIM_URL': 'https://...'}

        Örnek:
            etiket_settings = manager.get_app_settings('Etiket')
            baslik_url = etiket_settings.get('ETIKET_BASLIK_URL')
        """
        try:
            master_sheet = self.gc.open_by_key(self.MASTER_SPREADSHEET_ID)

            # Önce "Ayar" sayfasını dene, yoksa "Settings" dene
            try:
                settings_worksheet = master_sheet.worksheet('Ayar')
            except:
                settings_worksheet = master_sheet.worksheet('Settings')

            # Tüm ayarları oku
            settings = settings_worksheet.get_all_records()

            # Belirli app'e ait ayarları filtrele
            app_settings = {}
            for row in settings:
                row_app_name = str(row.get('App Name', '')).strip()
                key = str(row.get('Key', '')).strip()
                value = str(row.get('Value', '')).strip()

                # Bu app'e ait ayarları ekle
                if row_app_name == app_name and key:
                    app_settings[key] = value

            logger.info(f"{len(app_settings)} ayar yüklendi (App: {app_name})")
            return app_settings

        except Exception as e:
            logger.warning(f"App settings yükleme hatası ({app_name}): {e}")
            return {}

    def get_setting(self, key: str, default: str = '') -> str:
        """
        Belirli bir ayarı getir

        Args:
            key: Ayar anahtarı (örn: 'SQL_SERVER')
            default: Varsayılan değer (bulunamazsa)

        Returns:
            Ayar değeri
        """
        if not self.settings_cache:
            self.get_settings()

        return self.settings_cache.get(key, default)

    def refresh_config(self):
        """Config cache'ini temizle ve yeniden yükle (lokal + bellekte)"""
        self.config_cache = {}
        self.settings_cache = {}
        self.local_cache.clear()
        logger.info("Config cache cleared (local + memory)")


# ============================================================================
# YARDIMCI FONKSİYONLAR
# ============================================================================

def test_connection():
    """Service Account bağlantısını test et"""
    try:
        print("Service Account baglantisi test ediliyor...")
        manager = CentralConfigManager()

        print("\n[OK] Service Account basarili!")
        print(f"Service Account dosyasi: {manager.service_account_file}")

        # Config'leri yükle
        configs = manager.load_spreadsheet_configs()
        print(f"\n[CONFIG] Yuklenen spreadsheet'ler ({len(configs)} adet):")
        for app_name, sheet_id in configs.items():
            print(f"  - {app_name}: {sheet_id}")

        # Settings'i yükle
        settings = manager.get_settings()
        print(f"\n[SETTINGS] Yuklenen ayarlar ({len(settings)} adet):")
        for key, value in settings.items():
            # Şifreleri gizle
            if 'PASSWORD' in key.upper() or 'SECRET' in key.upper():
                value = '***'
            print(f"  - {key}: {value}")

        return True

    except Exception as e:
        print(f"\n[HATA] {e}")
        return False


if __name__ == "__main__":
    # Test
    test_connection()
