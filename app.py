from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os
import io
import json
from datetime import datetime
from werkzeug.utils import secure_filename
import tempfile
import logging
import pickle

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# CORS configuration - Tüm origin'lere izin
CORS(app, origins=["*"])

# Configuration - Büyük dosya desteği
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Temp file path for session persistence
TEMP_DATA_FILE = os.path.join(tempfile.gettempdir(), 'retailflow_data.pkl')

# BEDEN HARİTASI - JSON dosyasından yükle
def load_beden_haritasi():
    """Beden haritasını JSON dosyasından yükle"""
    try:
        json_path = os.path.join(os.path.dirname(__file__), 'beden_haritasi.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            beden_array = json.load(f)
        
        # Array formatını dictionary'ye çevir
        beden_haritasi = {}
        for item in beden_array:
            if isinstance(item, dict) and 'ÜRÜN ADI' in item and 'BEDEN ARALIĞI' in item:
                urun_adi = str(item['ÜRÜN ADI']).strip().upper()
                beden_araligi = str(item['BEDEN ARALIĞI']).strip()
                
                # Skip header row
                if urun_adi == 'ÜRÜN ADI' or beden_araligi == 'BEDEN ARALIĞI':
                    continue
                
                # Bedenleri array'e çevir
                if ',' in beden_araligi:
                    bedenler = [b.strip() for b in beden_araligi.split(',')]
                else:
                    bedenler = [beden_araligi]
                
                # Kategori belirleme
                if any(b in beden_araligi.upper() for b in ['XS', 'S', 'M', 'L']):
                    if '-' in beden_araligi:
                        kategori = 'kombine'
                    else:
                        kategori = 'tekstil'
                elif any(b in beden_araligi for b in ['28', '30', '32', '34']):
                    kategori = 'pantolon'
                elif any(b in beden_araligi for b in ['36', '38', '40', '42']):
                    kategori = 'ayakkabi'
                elif 'STD' in beden_araligi.upper():
                    kategori = 'standart'
                elif any(b in beden_araligi for b in ['Y', '110', '120', '130']):
                    kategori = 'cocuk'
                else:
                    kategori = 'diger'
                
                beden_haritasi[urun_adi] = {
                    'sizes': bedenler,
                    'category': kategori,
                    'original_range': beden_araligi
                }
        
        logger.info(f"Beden haritası yüklendi: {len(beden_haritasi)} ürün")
        return beden_haritasi
    except Exception as e:
        logger.error(f"Beden haritası yüklenirken hata: {e}")
        return {}

# Global değişken olarak yükle
BEDEN_HARITASI = load_beden_haritasi()

# Strategy configurations
STRATEGY_CONFIG = {
    'sakin': {
        'min_str_diff': 0.15,
        'min_inventory': 3,
        'max_transfer': 5,
        'description': 'Güvenli ve kontrollü transfer yaklaşımı'
    },
    'kontrollu': {
        'min_str_diff': 0.10,
        'min_inventory': 2,
        'max_transfer': 10,
        'description': 'Dengeli risk ve performans'
    },
    'agresif': {
        'min_str_diff': 0.08,
        'min_inventory': 1,
        'max_transfer': None,  # Sınırsız
        'description': 'Maksimum performans odaklı'
    }
}

class MagazaTransferSistemi:
    def __init__(self):
        self.data = None
        self.magazalar = []
        self.mevcut_analiz = None
        self.current_strategy = 'sakin'
        self.excluded_stores = []
        self.target_store = None  # Alan mağaza seçimi için
        self.transfer_type = 'global'  # 'global', 'targeted', 'size_completion'
        self.load_from_temp()

    def save_to_temp(self):
        """Veriyi geçici dosyaya kaydet"""
        try:
            with open(TEMP_DATA_FILE, 'wb') as f:
                pickle.dump({
                    'data': self.data,
                    'magazalar': self.magazalar,
                    'mevcut_analiz': self.mevcut_analiz,
                    'current_strategy': self.current_strategy,
                    'excluded_stores': self.excluded_stores,
                    'target_store': self.target_store,
                    'transfer_type': self.transfer_type
                }, f)
            logger.info("Data saved to temp file")
        except Exception as e:
            logger.error(f"Failed to save temp data: {e}")

    def load_from_temp(self):
        """Geçici dosyadan veriyi yükle"""
        try:
            if os.path.exists(TEMP_DATA_FILE):
                with open(TEMP_DATA_FILE, 'rb') as f:
                    temp_data = pickle.load(f)
                    self.data = temp_data.get('data')
                    self.magazalar = temp_data.get('magazalar', [])
                    self.mevcut_analiz = temp_data.get('mevcut_analiz')
                    self.current_strategy = temp_data.get('current_strategy', 'sakin')
                    self.excluded_stores = temp_data.get('excluded_stores', [])
                    self.target_store = temp_data.get('target_store')
                    self.transfer_type = temp_data.get('transfer_type', 'global')
                logger.info("Data loaded from temp file")
        except Exception as e:
            logger.error(f"Failed to load temp data: {e}")

    def clear_all_data(self):
        """Tüm veriyi temizle"""
        try:
            self.data = None
            self.magazalar = []
            self.mevcut_analiz = None
            self.current_strategy = 'sakin'
            self.excluded_stores = []
            self.target_store = None
            self.transfer_type = 'global'
            
            # Geçici dosyayı da sil
            if os.path.exists(TEMP_DATA_FILE):
                os.remove(TEMP_DATA_FILE)
                logger.info("Temp data file removed")
            
            logger.info("All data cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to clear data: {e}")
            return False

    def dosya_yukle_df(self, df):
        """DataFrame'i yükle ve işle"""
        try:
            # Sütun isimlerini temizle
            df.columns = df.columns.str.strip()
            
            logger.info(f"Bulunan sütunlar: {list(df.columns)}")
            
            gerekli_sutunlar = ['Depo Adı', 'Ürün Kodu', 'Ürün Adı', 'Satis', 'Envanter']
            eksik_sutunlar = [s for s in gerekli_sutunlar if s not in df.columns]
            
            if eksik_sutunlar:
                return False, f"Eksik sütunlar: {', '.join(eksik_sutunlar)}"
            
            df = df.dropna(subset=['Depo Adı'])
            df['Satis'] = pd.to_numeric(df['Satis'], errors='coerce').fillna(0)
            df['Envanter'] = pd.to_numeric(df['Envanter'], errors='coerce').fillna(0)
            
            # Negatif değerleri sıfırla
            df['Satis'] = df['Satis'].clip(lower=0)
            df['Envanter'] = df['Envanter'].clip(lower=0)
            
            self.data = df
            self.magazalar = df['Depo Adı'].unique().tolist()
            
            logger.info(f"Veri yüklendi: {len(df)} satır, {len(self.magazalar)} mağaza")
            
            result = {
                'message': f"Başarılı! {len(df):,} ürün, {len(self.magazalar)} mağaza yüklendi.",
                'satir_sayisi': len(df),
                'magaza_sayisi': len(self.magazalar),
                'magazalar': self.magazalar,
                'sutunlar': list(df.columns)
            }
            
            self.save_to_temp()
            return True, result
            
        except Exception as e:
            logger.error(f"Dosya yükleme hatası: {str(e)}")
            return False, f"Hata: {str(e)}"

    def magaza_metrikleri_hesapla(self):
        """Her mağaza için metrikleri hesapla"""
        if self.data is None:
            return {}

        metrikler = {}
        for magaza in self.magazalar:
            magaza_data = self.data[self.data['Depo Adı'] == magaza]
            toplam_satis = magaza_data['Satis'].sum()
            toplam_envanter = magaza_data['Envanter'].sum()

            metrikler[magaza] = {
                'toplam_satis': int(toplam_satis),
                'toplam_envanter': int(toplam_envanter),
                'satis_orani': float(toplam_satis / (toplam_satis + toplam_envanter)) if (toplam_satis + toplam_envanter) > 0 else 0,
                'envanter_fazlasi': int(toplam_envanter - toplam_satis),
                'urun_sayisi': len(magaza_data)
            }
        return metrikler

    def urun_anahtari_olustur(self, urun_adi, renk, beden):
        """Ürün adı + renk + beden kombinasyonu ile benzersiz anahtar oluştur"""
        urun_adi = str(urun_adi).strip().upper() if pd.notna(urun_adi) else ""
        renk = str(renk).strip().upper() if pd.notna(renk) else ""
        beden = str(beden).strip().upper() if pd.notna(beden) else ""
        return f"{urun_adi} {renk} {beden}".strip()

    def str_hesapla(self, satis, envanter):
        """Sell-Through Rate hesapla"""
        toplam = satis + envanter
        if toplam == 0:
            return 0
        return satis / toplam

    def str_bazli_transfer_hesapla(self, gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy='sakin'):
        """STR bazlı transfer miktarı hesapla - Strategy parametreli"""
        gonderen_str = self.str_hesapla(gonderen_satis, gonderen_envanter)
        alan_str = self.str_hesapla(alan_satis, alan_envanter)
        str_farki = alan_str - gonderen_str
        teorik_transfer = str_farki * gonderen_envanter
        
        # Strategy config al
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        
        # Koruma filtreleri - strategy bazlı
        max_transfer_40 = gonderen_envanter * 0.40
        
        # Strategy'ye göre minimum kalan
        min_kalan = gonderen_envanter - config['min_inventory']
        
        # Strategy'ye göre maksimum transfer
        if config['max_transfer'] is None:
            max_transfer_limit = float('inf')  # Sınırsız
        else:
            max_transfer_limit = config['max_transfer']
        
        transfer_miktari = min(teorik_transfer, max_transfer_40, min_kalan, max_transfer_limit)
        transfer_miktari = max(1, min(transfer_miktari, gonderen_envanter))
        
        # Hangi filtre uygulandığını belirle
        uygulanan_filtre = 'Teorik'
        if transfer_miktari == max_transfer_40:
            uygulanan_filtre = 'Max %40'
        elif transfer_miktari == min_kalan:
            uygulanan_filtre = f'Min {config["min_inventory"]} kalsın'
        elif transfer_miktari == max_transfer_limit and config['max_transfer'] is not None:
            uygulanan_filtre = f'Max {config["max_transfer"]} adet'
        
        return int(transfer_miktari), {
            'gonderen_str': round(gonderen_str * 100, 1),
            'alan_str': round(alan_str * 100, 1),
            'str_farki': round(str_farki * 100, 1),
            'teorik_transfer': round(teorik_transfer, 1),
            'uygulanan_filtre': uygulanan_filtre,
            'kullanilan_strateji': strategy
        }

    def transfer_kosulları_kontrol(self, gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy='sakin'):
        """STR bazlı transfer koşulları kontrol - Strategy parametreli"""
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        
        if alan_satis <= gonderen_satis:
            return False, f"Alan satış ({alan_satis}) ≤ Gönderen satış ({gonderen_satis})"
        
        if gonderen_envanter < config['min_inventory']:
            return False, f"Gönderen envanter yetersiz ({gonderen_envanter} < {config['min_inventory']})"
        
        gonderen_str = self.str_hesapla(gonderen_satis, gonderen_envanter)
        alan_str = self.str_hesapla(alan_satis, alan_envanter)
        str_farki = alan_str - gonderen_str
        
        if str_farki < config['min_str_diff']:
            return False, f"STR farkı yetersiz ({str_farki*100:.1f}% < {config['min_str_diff']*100}%)"
        
        transfer_miktari, detaylar = self.str_bazli_transfer_hesapla(
            gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
        )
        
        if transfer_miktari <= 0:
            return False, "Transfer miktarı hesaplanamadı"
        
        return True, f"STR: A{detaylar['alan_str']}%>G{detaylar['gonderen_str']}%, T:{transfer_miktari}"

    def get_urun_beden_araligi(self, urun_adi):
        """Ürün için beden aralığını beden haritasından al"""
        urun_adi_upper = urun_adi.strip().upper()
        if urun_adi_upper in BEDEN_HARITASI:
            return BEDEN_HARITASI[urun_adi_upper]['sizes']
        return None

    def beden_tamamlama_analizi_yap(self, target_store, excluded_stores=None):
        """Beden tamamlama analizi - DÜZELTME: Her eksik beden için sadece EN YÜKSEK envanterli mağazadan transfer"""
        if self.data is None:
            return None

        self.target_store = target_store
        self.transfer_type = 'size_completion'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores

        logger.info(f"Beden tamamlama analizi başlatılıyor... Hedef: {target_store} (En yüksek envanter mantığı)")
        
        transferler = []
        
        # Hedef mağazanın ürünlerini al
        target_data = self.data[self.data['Depo Adı'] == target_store]
        
        if target_data.empty:
            logger.warning(f"Hedef mağaza '{target_store}' için veri bulunamadı")
            return None

        # Her ürün için analiz
        for urun_adi in target_data['Ürün Adı'].unique():
            logger.info(f"Analiz ediliyor: {urun_adi}")
            
            # Beden haritasından tam aralığı al
            tam_beden_araligi = self.get_urun_beden_araligi(urun_adi)
            if not tam_beden_araligi:
                logger.warning(f"Beden haritasında bulunamadı: {urun_adi}")
                continue  # Bu ürün için beden haritası yok
            
            logger.info(f"{urun_adi} için tam beden aralığı: {tam_beden_araligi}")
            
            # Hedef mağazadaki mevcut bedenleri al
            urun_data_target = target_data[target_data['Ürün Adı'] == urun_adi]
            mevcut_bedenler = []
            
            for _, row in urun_data_target.iterrows():
                beden = str(row.get('Beden', '')).strip()
                if beden and beden != 'nan':
                    mevcut_bedenler.append(beden)
            
            logger.info(f"{urun_adi} - Hedef mağazada mevcut bedenler: {mevcut_bedenler}")
            
            # Eksik bedenleri tespit et
            eksik_bedenler = [b for b in tam_beden_araligi if b not in mevcut_bedenler]
            logger.info(f"{urun_adi} - Eksik bedenler: {eksik_bedenler}")
            
            # Her eksik beden için EN İYİ transfer kaynağını bul
            for eksik_beden in eksik_bedenler:
                logger.info(f"Eksik beden analizi: {urun_adi} - {eksik_beden}")
                
                # Diğer mağazalarda bu ürün+beden kombinasyonunu ara
                diger_magazalar_data = self.data[
                    (self.data['Depo Adı'] != target_store) &
                    (self.data['Ürün Adı'] == urun_adi) &
                    (self.data['Beden'].astype(str).str.strip() == eksik_beden) &
                    (~self.data['Depo Adı'].isin(excluded_stores))
                ]
                
                logger.info(f"{eksik_beden} bedeni için {len(diger_magazalar_data)} mağaza bulundu")
                
                if diger_magazalar_data.empty:
                    logger.warning(f"Eksik beden hiçbir mağazada yok: {urun_adi} - {eksik_beden}")
                    continue
                
                # EN YÜKSEK ENVANTERLI mağazayı bul
                en_yuksek_envanter = 0
                en_iyi_magaza = None
                
                for _, gonderen_row in diger_magazalar_data.iterrows():
                    gonderen_envanter = gonderen_row['Envanter']
                    if gonderen_envanter > en_yuksek_envanter:
                        en_yuksek_envanter = gonderen_envanter
                        en_iyi_magaza = gonderen_row
                
                # En iyi mağaza bulunduysa transfer öner
                if en_iyi_magaza is not None and en_yuksek_envanter > 0:
                    gonderen_magaza = en_iyi_magaza['Depo Adı']
                    gonderen_satis = en_iyi_magaza['Satis']
                    
                    logger.info(f"Transfer önerisi: {gonderen_magaza} → {target_store} | {urun_adi} {eksik_beden} | Envanter: {en_yuksek_envanter}")
                    
                    # Hedef mağaza için ortalama satış hesapla
                    if not urun_data_target.empty:
                        alan_satis = urun_data_target['Satis'].mean()
                    else:
                        alan_satis = 0
                    
                    # Transfer kaydı oluştur
                    transferler.append({
                        'urun_adi': urun_adi,
                        'urun_kodu': en_iyi_magaza.get('Ürün Kodu', ''),
                        'renk': en_iyi_magaza.get('Renk Açıklaması', ''),
                        'beden': eksik_beden,
                        'gonderen_magaza': gonderen_magaza,
                        'alan_magaza': target_store,
                        'transfer_miktari': 1,  # Sabit 1 adet
                        'gonderen_satis': int(gonderen_satis),
                        'gonderen_envanter': int(en_yuksek_envanter),
                        'alan_satis': int(alan_satis),
                        'alan_envanter': 0,  # Eksik beden için 0
                        'transfer_tipi': 'beden_tamamlama',
                        'eksik_beden': True,
                        'kullanilan_strateji': 'yok'
                    })
                else:
                    logger.warning(f"Transfer için uygun mağaza bulunamadı: {urun_adi} - {eksik_beden}")

        logger.info(f"Beden tamamlama analizi tamamlandı: {len(transferler)} transfer önerisi")
        
        result = {
            'analiz_tipi': 'beden_tamamlama',
            'strateji': 'beden_tamamlama',
            'target_store': target_store,
            'excluded_stores': excluded_stores,
            'transferler': transferler,
            'toplam_eksik_beden': len(transferler)
        }
        
        self.save_to_temp()
        return result

    def targeted_transfer_analizi_yap(self, target_store, strategy='sakin', excluded_stores=None):
        """Spesifik mağaza için transfer analizi - Sadece bu mağazayı alan olarak analiz et"""
        if self.data is None:
            return None

        self.current_strategy = strategy
        self.target_store = target_store
        self.transfer_type = 'targeted'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores

        logger.info(f"Spesifik mağaza analizi başlatılıyor... Hedef: {target_store}, Strateji: {strategy}")
        
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        transferler = []
        
        # Hedef mağazanın verilerini al
        target_data = self.data[self.data['Depo Adı'] == target_store]
        
        if target_data.empty:
            logger.warning(f"Hedef mağaza '{target_store}' için veri bulunamadı")
            return None

        # Diğer mağazalardan bu mağazaya transfer analizi
        diger_magazalar = [m for m in self.magazalar if m != target_store and m not in excluded_stores]
        
        # Her ürün için analiz
        tum_data = self.data.copy()
        tum_data['urun_anahtari'] = tum_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Ürün Adı'], 
                x.get('Renk Açıklaması', ''), 
                x.get('Beden', '')
            ), axis=1
        )
        
        # Hedef mağazadaki ürünleri al
        target_urun_anahtarlari = target_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Ürün Adı'], 
                x.get('Renk Açıklaması', ''), 
                x.get('Beden', '')
            ), axis=1
        ).unique()

        for urun_anahtari in target_urun_anahtarlari:
            # Hedef mağazadaki bu ürünün verilerini al
            target_urun_data = target_data[
                target_data.apply(lambda x: self.urun_anahtari_olustur(
                    x['Ürün Adı'], x.get('Renk Açıklaması', ''), x.get('Beden', '')
                ), axis=1) == urun_anahtari
            ]
            
            if target_urun_data.empty:
                continue
                
            target_row = target_urun_data.iloc[0]
            alan_satis = target_row['Satis']
            alan_envanter = target_row['Envanter']
            
            # Diğer mağazalarda aynı ürünü ara
            for gonderen_magaza in diger_magazalar:
                gonderen_urun_data = tum_data[
                    (tum_data['Depo Adı'] == gonderen_magaza) &
                    (tum_data['urun_anahtari'] == urun_anahtari)
                ]
                
                if gonderen_urun_data.empty:
                    continue
                    
                gonderen_row = gonderen_urun_data.iloc[0]
                gonderen_satis = gonderen_row['Satis']
                gonderen_envanter = gonderen_row['Envanter']
                
                # Transfer koşullarını kontrol et
                kosul_sonuc, kosul_mesaj = self.transfer_kosulları_kontrol(
                    gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
                )
                
                if kosul_sonuc:
                    transfer_miktari, str_detaylar = self.str_bazli_transfer_hesapla(
                        gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
                    )
                    
                    if transfer_miktari > 0:
                        transferler.append({
                            'urun_anahtari': urun_anahtari,
                            'urun_kodu': gonderen_row['Ürün Kodu'],
                            'urun_adi': gonderen_row['Ürün Adı'],
                            'renk': gonderen_row.get('Renk Açıklaması', ''),
                            'beden': gonderen_row.get('Beden', ''),
                            'gonderen_magaza': gonderen_magaza,
                            'alan_magaza': target_store,
                            'transfer_miktari': int(transfer_miktari),
                            'gonderen_satis': int(gonderen_satis),
                            'gonderen_envanter': int(gonderen_envanter),
                            'alan_satis': int(alan_satis),
                            'alan_envanter': int(alan_envanter),
                            'gonderen_str': str_detaylar['gonderen_str'],
                            'alan_str': str_detaylar['alan_str'],
                            'str_farki': str_detaylar['str_farki'],
                            'kullanilan_strateji': strategy,
                            'transfer_tipi': 'targeted'
                        })

        # STR farkına göre sırala
        transferler.sort(key=lambda x: x['str_farki'], reverse=True)
        
        logger.info(f"Spesifik mağaza analizi tamamlandı: {len(transferler)} transfer önerisi")
        
        result = {
            'analiz_tipi': 'targeted',
            'strateji': strategy,
            'target_store': target_store,
            'excluded_stores': excluded_stores,
            'transferler': transferler
        }
        
        self.save_to_temp()
        return result

    def global_transfer_analizi_yap(self, strategy='sakin', excluded_stores=None):
        """Global ürün bazlı transfer analizi - Strategy ve excluded_stores parametreli"""
        if self.data is None:
            return None

        # Strategy'yi kaydet
        self.current_strategy = strategy
        self.transfer_type = 'global'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores
        
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])

        logger.info(f"Global ürün bazlı STR transfer analizi başlatılıyor... Strateji: {strategy}")
        logger.info(f"İstisna mağazalar: {excluded_stores}")
        logger.info(f"Strateji parametreleri: {config}")
        
        metrikler = self.magaza_metrikleri_hesapla()
        transferler = []
        transfer_gereksiz = []

        # TÜM mağazaların ürünlerini grupla (ürün adı + renk + beden)
        tum_data = self.data.copy()
        
        # İstisna mağazaları filtrele
        if excluded_stores:
            tum_data = tum_data[~tum_data['Depo Adı'].isin(excluded_stores)]
            logger.info(f"İstisna mağazalar filtrelendi. Kalan veri: {len(tum_data)} satır")
        
        tum_data['urun_anahtari'] = tum_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Ürün Adı'], 
                x.get('Renk Açıklaması', ''), 
                x.get('Beden', '')
            ), axis=1
        )
        
        # Tüm benzersiz ürün anahtarlarını al
        tum_urun_anahtarlari = tum_data['urun_anahtari'].unique()
        
        logger.info(f"Toplam {len(tum_urun_anahtarlari)} benzersiz ürün grubu analiz ediliyor...")

        # Her ürün anahtarı için global optimizasyon
        for index, urun_anahtari in enumerate(tum_urun_anahtarlari):
            if (index + 1) % 100 == 0:
                logger.info(f"İşlenen: {index + 1}/{len(tum_urun_anahtarlari)}")

            # Bu ürünün tüm mağazalardaki durumunu analiz et
            urun_data = tum_data[tum_data['urun_anahtari'] == urun_anahtari]
            
            # Mağaza bazında grupla
            magaza_gruplari = urun_data.groupby('Depo Adı').agg({
                'Satis': 'sum',
                'Envanter': 'sum',
                'Ürün Adı': 'first',
                'Renk Açıklaması': 'first',
                'Beden': 'first',
                'Ürün Kodu': 'first'
            }).reset_index()

            # En az 2 mağazada olmalı transfer için
            if len(magaza_gruplari) < 2:
                continue

            # Her mağaza için STR hesapla
            magaza_str_listesi = []
            for _, magaza_grup in magaza_gruplari.iterrows():
                magaza = magaza_grup['Depo Adı']
                
                # İstisna mağaza kontrolü (extra güvenlik)
                if magaza in excluded_stores:
                    continue
                    
                satis = magaza_grup['Satis']
                envanter = magaza_grup['Envanter']
                str_value = self.str_hesapla(satis, envanter)
                
                magaza_str_listesi.append({
                    'magaza': magaza,
                    'satis': satis,
                    'envanter': envanter,
                    'str': str_value,
                    'urun_adi': magaza_grup['Ürün Adı'],
                    'renk': magaza_grup.get('Renk Açıklaması', ''),
                    'beden': magaza_grup.get('Beden', ''),
                    'urun_kodu': magaza_grup['Ürün Kodu']
                })

            # En az 2 mağaza kaldıysa devam et
            if len(magaza_str_listesi) < 2:
                continue

            # STR'a göre sırala (düşükten yükseğe)
            magaza_str_listesi.sort(key=lambda x: x['str'])
            
            # En düşük ve en yüksek STR'ı al
            en_dusuk_str = magaza_str_listesi[0]
            en_yuksek_str = magaza_str_listesi[-1]

            # Transfer koşullarını kontrol et - Strategy parametreli
            kosul_sonuc, kosul_mesaj = self.transfer_kosulları_kontrol(
                en_dusuk_str['satis'], en_dusuk_str['envanter'], 
                en_yuksek_str['satis'], en_yuksek_str['envanter'],
                strategy
            )
            
            if kosul_sonuc:
                # STR bazlı transfer miktarını hesapla - Strategy parametreli
                transfer_miktari, str_detaylar = self.str_bazli_transfer_hesapla(
                    en_dusuk_str['satis'], en_dusuk_str['envanter'],
                    en_yuksek_str['satis'], en_yuksek_str['envanter'],
                    strategy
                )
                
                if transfer_miktari > 0:
                    # Stok durumu STR bazında
                    alan_str_val = str_detaylar['alan_str']
                    if alan_str_val >= 80:
                        stok_durumu = 'YÜKSEK'
                    elif alan_str_val >= 50:
                        stok_durumu = 'NORMAL'
                    elif alan_str_val >= 20:
                        stok_durumu = 'DÜŞÜK'
                    else:
                        stok_durumu = 'KRİTİK'
                    
                    transferler.append({
                        'urun_anahtari': urun_anahtari,
                        'urun_kodu': en_dusuk_str['urun_kodu'],
                        'urun_adi': en_dusuk_str['urun_adi'],
                        'renk': en_dusuk_str['renk'],
                        'beden': en_dusuk_str['beden'],
                        'gonderen_magaza': en_dusuk_str['magaza'],
                        'alan_magaza': en_yuksek_str['magaza'],
                        'transfer_miktari': int(transfer_miktari),
                        'gonderen_satis': int(en_dusuk_str['satis']),
                        'gonderen_envanter': int(en_dusuk_str['envanter']),
                        'alan_satis': int(en_yuksek_str['satis']),
                        'alan_envanter': int(en_yuksek_str['envanter']),
                        'gonderen_str': str_detaylar['gonderen_str'],
                        'alan_str': str_detaylar['alan_str'],
                        'str_farki': str_detaylar['str_farki'],
                        'teorik_transfer': str_detaylar['teorik_transfer'],
                        'uygulanan_filtre': str_detaylar['uygulanan_filtre'],
                        'kullanilan_strateji': str_detaylar['kullanilan_strateji'],
                        'alan_stok_durumu': stok_durumu,
                        'magaza_sayisi': len(magaza_str_listesi),
                        'min_str': round(en_dusuk_str['str'] * 100, 1),
                        'max_str': round(en_yuksek_str['str'] * 100, 1),
                        'salis_farki': int(en_yuksek_str['satis'] - en_dusuk_str['satis']),
                        'envanter_farki': int(en_dusuk_str['envanter'] - en_yuksek_str['envanter'])
                    })
            else:
                # Transfer gerekmeyen ürünleri kaydet
                str_ortalama = sum(m['str'] for m in magaza_str_listesi) / len(magaza_str_listesi)
                str_fark = max(m['str'] for m in magaza_str_listesi) - min(m['str'] for m in magaza_str_listesi)
                
                transfer_gereksiz.append({
                    'urun_anahtari': urun_anahtari,
                    'urun_adi': magaza_str_listesi[0]['urun_adi'],
                    'renk': magaza_str_listesi[0]['renk'],
                    'beden': magaza_str_listesi[0]['beden'],
                    'magaza_sayisi': len(magaza_str_listesi),
                    'ortalama_str': round(str_ortalama * 100, 1),
                    'str_fark': round(str_fark * 100, 1),
                    'red_nedeni': kosul_mesaj
                })

        # STR farkına göre sırala (yüksek fark = daha öncelikli)
        transferler.sort(key=lambda x: x['str_farki'], reverse=True)

        logger.info(f"Global analiz tamamlandı ({strategy}): {len(transferler)} transfer, {len(transfer_gereksiz)} red")
        if excluded_stores:
            logger.info(f"İstisna mağazalar: {excluded_stores}")

        result = {
            'analiz_tipi': 'global',
            'strateji': strategy,
            'strateji_parametreleri': config,
            'excluded_stores': excluded_stores,
            'excluded_count': len(excluded_stores),
            'magaza_metrikleri': metrikler,
            'transferler': transferler,
            'transfer_gereksiz': transfer_gereksiz
        }
        
        self.save_to_temp()
        return result

# Global sistem instance
sistem = MagazaTransferSistemi()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'RetailFlow Transfer API',
        'version': '6.0.0',
        'timestamp': datetime.now().isoformat(),
        'data_loaded': sistem.data is not None,
        'store_count': len(sistem.magazalar) if sistem.magazalar else 0,
        'current_strategy': sistem.current_strategy,
        'transfer_type': sistem.transfer_type,
        'target_store': sistem.target_store,
        'excluded_stores': sistem.excluded_stores,
        'available_strategies': list(STRATEGY_CONFIG.keys()),
        'beden_haritasi_loaded': len(BEDEN_HARITASI) > 0,
        'beden_haritasi_count': len(BEDEN_HARITASI)
    })

@app.route('/upload', methods=['POST'])
def upload_file():
    """Dosya yükleme endpoint'i"""
    try:
        logger.info("File upload request received")
        
        if 'file' not in request.files:
            return jsonify({'error': 'Dosya seçilmedi'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Dosya seçilmedi'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Geçersiz dosya formatı! Excel (.xlsx, .xls) veya CSV dosyası yükleyin.'}), 400
        
        filename = secure_filename(file.filename)
        logger.info(f"Processing file: {filename}")
        
        # Excel veya CSV oku
        try:
            if filename.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='cp1254')
            else:
                df = pd.read_excel(file, engine='openpyxl' if filename.endswith('.xlsx') else None)
            
            logger.info(f"File read successfully: {len(df)} rows")
        except Exception as e:
            logger.error(f"File reading error: {str(e)}")
            return jsonify({'error': f'Dosya okuma hatası: {str(e)}'}), 400
        
        # Sisteme yükle
        success, result = sistem.dosya_yukle_df(df)
        
        if success:
            logger.info("Data loaded successfully")
            return jsonify({
                'success': True,
                'filename': filename,
                'data': result
            })
        else:
            logger.error(f"Data loading failed: {result}")
            return jsonify({'error': result}), 400
            
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': f'Dosya yükleme hatası: {str(e)}'}), 500

@app.route('/remove-file', methods=['POST'])
def remove_file():
    """Yüklenen dosyayı kaldırma endpoint'i"""
    try:
        logger.info("File removal request received")
        
        if sistem.data is None:
            logger.warning("No file to remove")
            return jsonify({'error': 'Kaldırılacak dosya yok'}), 400
        
        # Tüm veriyi temizle
        success = sistem.clear_all_data()
        
        if success:
            logger.info("File and all data removed successfully")
            return jsonify({
                'success': True,
                'message': 'Dosya ve tüm veriler başarıyla kaldırıldı',
                'timestamp': datetime.now().isoformat()
            })
        else:
            logger.error("Failed to remove file and data")
            return jsonify({'error': 'Dosya kaldırma işlemi başarısız'}), 500
            
    except Exception as e:
        logger.error(f"File removal error: {str(e)}")
        return jsonify({'error': f'Dosya kaldırma hatası: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze_data():
    """Transfer analizi - Global, Targeted veya Size Completion"""
    try:
        logger.info("Analysis request received")
        
        if sistem.data is None:
            logger.warning("No data available for analysis")
            return jsonify({'error': 'Önce bir dosya yükleyin'}), 400
        
        # Request parametrelerini al
        request_data = request.get_json() or {}
        strategy = request_data.get('strategy', 'sakin')
        excluded_stores = request_data.get('excluded_stores', [])
        transfer_type = request_data.get('transfer_type', 'global')  # global, targeted, size_completion
        target_store = request_data.get('target_store', None)
        
        # Parametreleri validate et
        if strategy not in STRATEGY_CONFIG:
            logger.warning(f"Invalid strategy: {strategy}")
            strategy = 'sakin'
        
        if not isinstance(excluded_stores, list):
            excluded_stores = []
        
        # Excluded stores'ları mevcut mağaza listesiyle filtrele
        valid_excluded_stores = [store for store in excluded_stores if store in sistem.magazalar]
        
        logger.info(f"Starting {transfer_type} analysis... Strategy: {strategy}")
        logger.info(f"Target store: {target_store}")
        logger.info(f"Excluded stores: {valid_excluded_stores}")
        
        # Transfer tipine göre analiz yap
        if transfer_type == 'size_completion':
            if not target_store or target_store not in sistem.magazalar:
                return jsonify({'error': 'Beden tamamlama için geçerli bir hedef mağaza seçin'}), 400
            
            # Beden tamamlama için strateji kullanılmaz
            results = sistem.beden_tamamlama_analizi_yap(target_store, valid_excluded_stores)
        
        elif transfer_type == 'targeted':
            if not target_store or target_store not in sistem.magazalar:
                return jsonify({'error': 'Spesifik mağaza analizi için geçerli bir hedef mağaza seçin'}), 400
            
            results = sistem.targeted_transfer_analizi_yap(target_store, strategy, valid_excluded_stores)
        
        else:  # global
            results = sistem.global_transfer_analizi_yap(strategy, valid_excluded_stores)
        
        if results:
            sistem.mevcut_analiz = results
            
            # Sonuçları limitli şekilde gönder (JSON boyutunu küçült)
            limited_results = {
                'analiz_tipi': results['analiz_tipi'],
                'strateji': results['strateji'],
                'target_store': results.get('target_store'),
                'excluded_stores': results.get('excluded_stores', []),
                'excluded_count': results.get('excluded_count', 0),
                'transferler': results['transferler'][:50] if results.get('transferler') else [],
                'toplam_transfer_sayisi': len(results.get('transferler', []))
            }
            
            # Global analiz için ek bilgiler
            if transfer_type == 'global':
                limited_results['strateji_parametreleri'] = results.get('strateji_parametreleri')
                limited_results['magaza_metrikleri'] = results.get('magaza_metrikleri')
                limited_results['transfer_gereksiz'] = results.get('transfer_gereksiz', [])[:20]
                limited_results['toplam_gereksiz_sayisi'] = len(results.get('transfer_gereksiz', []))
            
            logger.info(f"{transfer_type.capitalize()} analysis completed ({strategy}): {len(results.get('transferler', []))} total transfers")
            if valid_excluded_stores:
                logger.info(f"Excluded {len(valid_excluded_stores)} stores from analysis")
            
            return jsonify({
                'success': True,
                'results': limited_results
            })
        else:
            logger.error("Analysis returned no results")
            return jsonify({'error': 'Analiz başarısız'}), 500
            
    except Exception as e:
        logger.error(f"Analysis error: {str(e)}")
        return jsonify({'error': f'Analiz hatası: {str(e)}'}), 500

@app.route('/export/excel', methods=['POST'])
def export_excel():
    """Excel export - Transfer tipine göre özelleştirilmiş"""
    try:
        logger.info("Excel export request received")
        
        if not sistem.mevcut_analiz:
            logger.warning("No analysis results available for export")
            return jsonify({'error': 'Analiz sonucu bulunamadı'}), 400
        
        transferler = sistem.mevcut_analiz['transferler']
        analiz_tipi = sistem.mevcut_analiz.get('analiz_tipi', 'global')
        strategy = sistem.mevcut_analiz.get('strateji', 'sakin')
        target_store = sistem.mevcut_analiz.get('target_store', '')
        
        logger.info(f"Exporting {len(transferler)} transfers to Excel (Type: {analiz_tipi}, Strategy: {strategy})")
        
        # Excel dosyası oluştur
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Ana transfer sayfası
            if transferler:
                df_transfer = pd.DataFrame(transferler)
                
                # Temel sütunlar
                if analiz_tipi == 'size_completion':
                    selected_columns = {
                        'urun_adi': 'Ürün Adı',
                        'renk': 'Renk',
                        'beden': 'Eksik Beden',
                        'gonderen_magaza': 'Gönderen Mağaza',
                        'alan_magaza': 'Hedef Mağaza',
                        'transfer_miktari': 'Transfer Miktarı',
                        'gonderen_envanter': 'Gönderen Envanter',
                        'kullanilan_strateji': 'Strateji'
                    }
                    sheet_name = f'{target_store} Beden Tamamlama'
                else:
                    selected_columns = {
                        'urun_kodu': 'Ürün Kodu',
                        'urun_adi': 'Ürün Adı',
                        'renk': 'Renk',
                        'beden': 'Beden',
                        'gonderen_magaza': 'Gönderen Mağaza',
                        'alan_magaza': 'Alan Mağaza',
                        'transfer_miktari': 'Transfer Miktarı',
                        'str_farki': 'STR Farkı (%)',
                        'kullanilan_strateji': 'Strateji'
                    }
                    sheet_name = f'{target_store} Transferleri' if analiz_tipi == 'targeted' else 'Transfer Önerileri'
                
                # Mevcut sütunları filtrele
                available_columns = {k: v for k, v in selected_columns.items() if k in df_transfer.columns}
                df_export = df_transfer[list(available_columns.keys())].copy()
                df_export = df_export.rename(columns=available_columns)
                
                df_export.to_excel(writer, index=False, sheet_name=sheet_name[:31])  # Excel sheet name limit
            
            # Analiz özeti sayfası
            summary_info = {
                'Analiz Tipi': [analiz_tipi.upper()],
                'Kullanılan Strateji': [strategy],
                'Hedef Mağaza': [target_store or 'Tüm Mağazalar'],
                'Toplam Transfer': [len(transferler)],
                'İstisna Mağaza Sayısı': [len(sistem.excluded_stores)],
                'Analiz Tarihi': [datetime.now().strftime('%Y-%m-%d %H:%M')]
            }
            
            if sistem.excluded_stores:
                summary_info['İstisna Mağazalar'] = [', '.join(sistem.excluded_stores)]
            
            df_summary = pd.DataFrame(summary_info)
            df_summary.to_excel(writer, index=False, sheet_name='Analiz Özeti')
        
        output.seek(0)
        
        # Dosya adı oluştur
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if analiz_tipi == 'size_completion':
            filename = f'beden_tamamlama_{target_store}_{strategy}_{timestamp}.xlsx'
        elif analiz_tipi == 'targeted':
            filename = f'targeted_{target_store}_{strategy}_{timestamp}.xlsx'
        else:
            filename = f'global_transfer_{strategy}_{timestamp}.xlsx'
        
        # Dosya adını temizle (Windows uyumluluğu için)
        filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
        
        logger.info(f"Excel file created: {filename}")
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        return jsonify({'error': f'Export hatası: {str(e)}'}), 500

@app.route('/stores', methods=['GET'])
def get_stores():
    """Mağaza listesi"""
    try:
        if not sistem.magazalar:
            return jsonify({'error': 'Mağaza verisi bulunamadı'}), 400
        
        metrikler = sistem.magaza_metrikleri_hesapla()
        stores = []
        
        for magaza in sistem.magazalar:
            if magaza in metrikler:
                m = metrikler[magaza]
                str_oran = m['satis_orani'] * 100
                stores.append({
                    'name': magaza,
                    'sales': m['toplam_satis'],
                    'inventory': m['toplam_envanter'],
                    'str_rate': round(str_oran, 1),
                    'product_count': m['urun_sayisi'],
                    'excess_inventory': m['envanter_fazlasi']
                })
        
        return jsonify({'success': True, 'stores': stores})
        
    except Exception as e:
        logger.error(f"Stores error: {str(e)}")
        return jsonify({'error': f'Mağaza verisi hatası: {str(e)}'}), 500

@app.route('/strategies', methods=['GET'])
def get_strategies():
    """Mevcut stratejileri listele"""
    try:
        strategies = []
        for key, config in STRATEGY_CONFIG.items():
            strategies.append({
                'name': key,
                'description': config['description'],
                'parameters': {
                    'min_str_diff_percent': config['min_str_diff'] * 100,
                    'min_inventory': config['min_inventory'],
                    'max_transfer': config['max_transfer']
                }
            })
        
        return jsonify({
            'success': True,
            'strategies': strategies,
            'current_strategy': sistem.current_strategy,
            'transfer_type': sistem.transfer_type,
            'target_store': sistem.target_store
        })
        
    except Exception as e:
        logger.error(f"Strategies error: {str(e)}")
        return jsonify({'error': f'Strateji verisi hatası: {str(e)}'}), 500

# Error handlers
@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'Dosya boyutu çok büyük (max 100MB)'}), 413

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Sunucu hatası'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint bulunamadı'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug_mode = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info(f"Starting RetailFlow API v6.0 on port {port}")
    logger.info(f"Beden haritası yüklendi: {len(BEDEN_HARITASI)} ürün")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
