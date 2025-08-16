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

# CORS configuration - Tum origin'lere izin
CORS(app, origins=["*"])

# Configuration - Buyuk dosya destegi
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Temp file path for session persistence
TEMP_DATA_FILE = os.path.join(tempfile.gettempdir(), 'retailflow_data.pkl')

# Strategy configurations
STRATEGY_CONFIG = {
    'sakin': {
        'min_str_diff': 0.15,
        'min_inventory': 3,
        'max_transfer': 5,
        'description': 'Guvenli ve kontrollu transfer yaklasimi'
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
        'max_transfer': None,  # sinirsiz
        'description': 'Maksimum performans odakli'
    }
}

class MagazaTransferSistemi:
    def __init__(self):
        self.data = None
        self.magazalar = []
        self.mevcut_analiz = None
        self.current_strategy = 'sakin'
        self.excluded_stores = []
        self.target_store = None  # Alan magaza secimi icin
        self.transfer_type = 'global'  # 'global', 'targeted', 'size_completion'
        self.load_from_temp()

    def save_to_temp(self):
        """Veriyi gecici dosyaya kaydet"""
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
        """Gecici dosyadan veriyi yukle"""
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
        """Tum veriyi temizle"""
        try:
            self.data = None
            self.magazalar = []
            self.mevcut_analiz = None
            self.current_strategy = 'sakin'
            self.excluded_stores = []
            self.target_store = None
            self.transfer_type = 'global'
            
            # Gecici dosyayi da sil
            if os.path.exists(TEMP_DATA_FILE):
                os.remove(TEMP_DATA_FILE)
                logger.info("Temp data file removed")
            
            logger.info("All data cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to clear data: {e}")
            return False

    def dosya_yukle_df(self, df):
        """DataFrame'i yukle ve isle"""
        try:
            # Sutun isimlerini temizle
            df.columns = df.columns.str.strip()
            
            logger.info(f"Bulunan sutunlar: {list(df.columns)}")
            
            # TURKCE SUTUN ADLARINI INGILIZCE'YE CEVIR
            column_mapping = {
                'Depo Adı': 'Depo Adi',
                'Ürün Kodu': 'Urun Kodu', 
                'Ürün Adı': 'Urun Adi',
                'Renk Açıklaması': 'Renk Aciklamasi',
                # Beden ve Satis, Envanter zaten problem yok
            }
            
            # Sutun adlarini yeniden adlandir
            df = df.rename(columns=column_mapping)
            logger.info(f"Sutunlar cevrildikten sonra: {list(df.columns)}")
            
            gerekli_sutunlar = ['Depo Adi', 'Urun Kodu', 'Urun Adi', 'Satis', 'Envanter']
            eksik_sutunlar = [s for s in gerekli_sutunlar if s not in df.columns]
            
            if eksik_sutunlar:
                return False, f"Eksik sutunlar: {', '.join(eksik_sutunlar)}"
            
            df = df.dropna(subset=['Depo Adi'])
            df['Satis'] = pd.to_numeric(df['Satis'], errors='coerce').fillna(0)
            df['Envanter'] = pd.to_numeric(df['Envanter'], errors='coerce').fillna(0)
            
            # Negatif degerleri sifirla
            df['Satis'] = df['Satis'].clip(lower=0)
            df['Envanter'] = df['Envanter'].clip(lower=0)
            
            self.data = df
            self.magazalar = df['Depo Adi'].unique().tolist()
            
            logger.info(f"Veri yuklendi: {len(df)} satir, {len(self.magazalar)} magaza")
            
            result = {
                'message': f"Basarili! {len(df):,} urun, {len(self.magazalar)} magaza yuklendi.",
                'satir_sayisi': len(df),
                'magaza_sayisi': len(self.magazalar),
                'magazalar': self.magazalar,
                'sutunlar': list(df.columns)
            }
            
            self.save_to_temp()
            return True, result
            
        except Exception as e:
            logger.error(f"Dosya yukleme hatasi: {str(e)}")
            return False, f"Hata: {str(e)}"

    def magaza_metrikleri_hesapla(self):
        """Her magaza icin metrikleri hesapla"""
        if self.data is None:
            return {}

        metrikler = {}
        for magaza in self.magazalar:
            magaza_data = self.data[self.data['Depo Adi'] == magaza]
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
        """Urun adi + renk + beden kombinasyonu ile benzersiz anahtar olustur"""
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
        """STR bazli transfer miktari hesapla - Strategy parametreli"""
        gonderen_str = self.str_hesapla(gonderen_satis, gonderen_envanter)
        alan_str = self.str_hesapla(alan_satis, alan_envanter)
        str_farki = alan_str - gonderen_str
        teorik_transfer = str_farki * gonderen_envanter
        
        # Strategy config al
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        
        # Koruma filtreleri - strategy bazli
        max_transfer_40 = gonderen_envanter * 0.40
        
        # Strategy'ye gore minimum kalan
        min_kalan = gonderen_envanter - config['min_inventory']
        
        # Strategy'ye gore maksimum transfer
        if config['max_transfer'] is None:
            max_transfer_limit = float('inf')  # Sinirsiz
        else:
            max_transfer_limit = config['max_transfer']
        
        transfer_miktari = min(teorik_transfer, max_transfer_40, min_kalan, max_transfer_limit)
        transfer_miktari = max(1, min(transfer_miktari, gonderen_envanter))
        
        # Hangi filtre uygulandigini belirle
        uygulanan_filtre = 'Teorik'
        if transfer_miktari == max_transfer_40:
            uygulanan_filtre = 'Max %40'
        elif transfer_miktari == min_kalan:
            uygulanan_filtre = f'Min {config["min_inventory"]} kalsin'
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

    def transfer_kosullari_kontrol(self, gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy='sakin'):
        """STR bazli transfer kosullari kontrol - Strategy parametreli"""
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        
        if alan_satis <= gonderen_satis:
            return False, f"Alan satis ({alan_satis}) <= Gonderen satis ({gonderen_satis})"
        
        if gonderen_envanter < config['min_inventory']:
            return False, f"Gonderen envanter yetersiz ({gonderen_envanter} < {config['min_inventory']})"
        
        gonderen_str = self.str_hesapla(gonderen_satis, gonderen_envanter)
        alan_str = self.str_hesapla(alan_satis, alan_envanter)
        str_farki = alan_str - gonderen_str
        
        if str_farki < config['min_str_diff']:
            return False, f"STR farki yetersiz ({str_farki*100:.1f}% < {config['min_str_diff']*100}%)"
        
        transfer_miktari, detaylar = self.str_bazli_transfer_hesapla(
            gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
        )
        
        if transfer_miktari <= 0:
            return False, "Transfer miktari hesaplanamadi"
        
        return True, f"STR: A{detaylar['alan_str']}%>G{detaylar['gonderen_str']}%, T:{transfer_miktari}"

    def beden_tamamlama_analizi_yap(self, target_store, excluded_stores=None):
        """DUZELTME: Global transfer mantigi ile urun anahtari bazli beden tamamlama"""
        if self.data is None:
            return None

        self.target_store = target_store
        self.transfer_type = 'size_completion'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores

        logger.info(f"Hedef magaza beden tamamlama analizi baslatiliyor... Hedef: {target_store}")
        
        transferler = []
        
        # Hedef magazanin urunlerini al
        target_data = self.data[self.data['Depo Adi'] == target_store]
        
        if target_data.empty:
            logger.warning(f"Hedef magaza '{target_store}' icin veri bulunamadi")
            return None

        # 1. TUM VERI icin urun anahtarlari olustur (Global transfer mantigi)
        logger.info("Tum veri icin urun anahtarlari olusturuluyor...")
        self.data['urun_anahtari'] = self.data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Urun Adi'], 
                x.get('Renk Aciklamasi', ''), 
                x.get('Beden', '')
            ), axis=1
        )
        
        # 2. Hedef magaza icin de urun anahtarlari olustur
        target_data['urun_anahtari'] = target_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Urun Adi'], 
                x.get('Renk Aciklamasi', ''), 
                x.get('Beden', '')
            ), axis=1
        )

        # 3. Hedef magazada envanter=0 olan urun anahtarlarini bul
        sifir_envanter = target_data[target_data['Envanter'] == 0]
        logger.info(f"{target_store}'da envanter=0 olan {len(sifir_envanter)} urun kombinasyonu bulundu")
        
        if sifir_envanter.empty:
            logger.info(f"{target_store}'da hic eksik urun yok!")
            result = {
                'analiz_tipi': 'beden_tamamlama',
                'strateji': 'urun_anahtari_bazli',
                'target_store': target_store,
                'excluded_stores': excluded_stores,
                'transferler': [],
                'toplam_eksik_beden': 0
            }
            self.save_to_temp()
            return result
        
        # 4. Her eksik urun anahtari icin en iyi kaynak magazayi bul
        for index, eksik_row in sifir_envanter.iterrows():
            eksik_urun_anahtari = eksik_row['urun_anahtari']
            urun_adi = eksik_row['Urun Adi']
            renk = eksik_row.get('Renk Aciklamasi', '')
            beden = str(eksik_row.get('Beden', '')).strip()
            urun_kodu = eksik_row.get('Urun Kodu', '')
            alan_satis = eksik_row['Satis']
            
            logger.info(f"Eksik urun analizi: '{eksik_urun_anahtari}' (Alan satis: {alan_satis})")
            
            # 5. Ayni urun anahtarini diger magazalarda ara (Global transfer mantigi)
            kaynak_magazalar = self.data[
                (self.data['Depo Adi'] != target_store) &
                (self.data['urun_anahtari'] == eksik_urun_anahtari) &
                (self.data['Envanter'] > 0) &
                (~self.data['Depo Adi'].isin(excluded_stores))
            ]
            
            logger.info(f"'{eksik_urun_anahtari}' icin {len(kaynak_magazalar)} kaynak magaza bulundu")
            
            if kaynak_magazalar.empty:
                logger.warning(f"'{eksik_urun_anahtari}' icin kaynak magaza bulunamadi")
                continue
            
            # 6. EN YUKSEK ENVANTERLI magazayi sec
            en_iyi_kaynak = kaynak_magazalar.loc[kaynak_magazalar['Envanter'].idxmax()]
            gonderen_magaza = en_iyi_kaynak['Depo Adi']
            gonderen_envanter = en_iyi_kaynak['Envanter']
            gonderen_satis = en_iyi_kaynak['Satis']
            
            logger.info(f"Transfer onerisi: {gonderen_magaza}({gonderen_envanter} adet) -> {target_store}")
            logger.info(f"   Urun: {urun_adi} | Renk: {renk} | Beden: {beden}")
            
            # 7. Transfer kaydi olustur
            transferler.append({
                'urun_adi': urun_adi,
                'urun_kodu': urun_kodu,
                'renk': renk,
                'beden': beden,
                'gonderen_magaza': gonderen_magaza,
                'alan_magaza': target_store,
                'transfer_miktari': 1,
                'gonderen_satis': int(gonderen_satis),
                'gonderen_envanter': int(gonderen_envanter),
                'alan_satis': int(alan_satis),
                'alan_envanter': 0,
                'transfer_tipi': 'urun_anahtari_beden_tamamlama',
                'eksik_beden': True,
                'kullanilan_strateji': 'urun_anahtari_bazli',
                'urun_anahtari': eksik_urun_anahtari
            })

        logger.info(f"Urun anahtari bazli beden tamamlama tamamlandi: {len(transferler)} transfer onerisi")
        
        # Transferleri urun anahtarina gore sirala
        transferler.sort(key=lambda x: x['urun_anahtari'])
        
        result = {
            'analiz_tipi': 'beden_tamamlama',
            'strateji': 'urun_anahtari_bazli',
            'target_store': target_store,
            'excluded_stores': excluded_stores,
            'transferler': transferler,
            'toplam_eksik_beden': len(transferler)
        }
        
        self.save_to_temp()
        return result

    def targeted_transfer_analizi_yap(self, target_store, strategy='sakin', excluded_stores=None):
        """Spesifik magaza icin transfer analizi - Sadece bu magazayi alan olarak analiz et"""
        if self.data is None:
            return None

        self.current_strategy = strategy
        self.target_store = target_store
        self.transfer_type = 'targeted'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores

        logger.info(f"Spesifik magaza analizi baslatiliyor... Hedef: {target_store}, Strateji: {strategy}")
        
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])
        transferler = []
        
        # Hedef magazanin verilerini al
        target_data = self.data[self.data['Depo Adi'] == target_store]
        
        if target_data.empty:
            logger.warning(f"Hedef magaza '{target_store}' icin veri bulunamadi")
            return None

        # Diger magazalardan bu magazaya transfer analizi
        diger_magazalar = [m for m in self.magazalar if m != target_store and m not in excluded_stores]
        
        # Her urun icin analiz
        tum_data = self.data.copy()
        tum_data['urun_anahtari'] = tum_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Urun Adi'], 
                x.get('Renk Aciklamasi', ''), 
                x.get('Beden', '')
            ), axis=1
        )
        
        # Hedef magazadaki urunleri al
        target_urun_anahtarlari = target_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Urun Adi'], 
                x.get('Renk Aciklamasi', ''), 
                x.get('Beden', '')
            ), axis=1
        ).unique()

        for urun_anahtari in target_urun_anahtarlari:
            # Hedef magazadaki bu urunun verilerini al
            target_urun_data = target_data[
                target_data.apply(lambda x: self.urun_anahtari_olustur(
                    x['Urun Adi'], x.get('Renk Aciklamasi', ''), x.get('Beden', '')
                ), axis=1) == urun_anahtari
            ]
            
            if target_urun_data.empty:
                continue
                
            target_row = target_urun_data.iloc[0]
            alan_satis = target_row['Satis']
            alan_envanter = target_row['Envanter']
            
            # Diger magazalarda ayni urunu ara
            for gonderen_magaza in diger_magazalar:
                gonderen_urun_data = tum_data[
                    (tum_data['Depo Adi'] == gonderen_magaza) &
                    (tum_data['urun_anahtari'] == urun_anahtari)
                ]
                
                if gonderen_urun_data.empty:
                    continue
                    
                gonderen_row = gonderen_urun_data.iloc[0]
                gonderen_satis = gonderen_row['Satis']
                gonderen_envanter = gonderen_row['Envanter']
                
                # Transfer kosullarini kontrol et
                kosul_sonuc, kosul_mesaj = self.transfer_kosullari_kontrol(
                    gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
                )
                
                if kosul_sonuc:
                    transfer_miktari, str_detaylar = self.str_bazli_transfer_hesapla(
                        gonderen_satis, gonderen_envanter, alan_satis, alan_envanter, strategy
                    )
                    
                    if transfer_miktari > 0:
                        transferler.append({
                            'urun_anahtari': urun_anahtari,
                            'urun_kodu': gonderen_row['Urun Kodu'],
                            'urun_adi': gonderen_row['Urun Adi'],
                            'renk': gonderen_row.get('Renk Aciklamasi', ''),
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

        # STR farkina gore sirala
        transferler.sort(key=lambda x: x['str_farki'], reverse=True)
        
        logger.info(f"Spesifik magaza analizi tamamlandi: {len(transferler)} transfer onerisi")
        
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
        """Global urun bazli transfer analizi - Strategy ve excluded_stores parametreli"""
        if self.data is None:
            return None

        # Strategy'yi kaydet
        self.current_strategy = strategy
        self.transfer_type = 'global'
        if excluded_stores is None:
            excluded_stores = []
        self.excluded_stores = excluded_stores
        
        config = STRATEGY_CONFIG.get(strategy, STRATEGY_CONFIG['sakin'])

        logger.info(f"Global urun bazli STR transfer analizi baslatiliyor... Strateji: {strategy}")
        logger.info(f"Istisna magazalar: {excluded_stores}")
        logger.info(f"Strateji parametreleri: {config}")
        
        metrikler = self.magaza_metrikleri_hesapla()
        transferler = []
        transfer_gereksiz = []

        # TUM magazalarinin urunlerini grupla (urun adi + renk + beden)
        tum_data = self.data.copy()
        
        # Istisna magazalari filtrele
        if excluded_stores:
            tum_data = tum_data[~tum_data['Depo Adi'].isin(excluded_stores)]
            logger.info(f"Istisna magazalar filtrelendi. Kalan veri: {len(tum_data)} satir")
        
        tum_data['urun_anahtari'] = tum_data.apply(
            lambda x: self.urun_anahtari_olustur(
                x['Urun Adi'], 
                x.get('Renk Aciklamasi', ''), 
                x.get('Beden', '')
            ), axis=1
        )
        
        # Tum benzersiz urun anahtarlarini al
        tum_urun_anahtarlari = tum_data['urun_anahtari'].unique()
        
        logger.info(f"Toplam {len(tum_urun_anahtarlari)} benzersiz urun grubu analiz ediliyor...")

        # Her urun anahtari icin global optimizasyon
        for index, urun_anahtari in enumerate(tum_urun_anahtarlari):
            if (index + 1) % 100 == 0:
                logger.info(f"Islenen: {index + 1}/{len(tum_urun_anahtarlari)}")

            # Bu urunun tum magazalardaki durumunu analiz et
            urun_data = tum_data[tum_data['urun_anahtari'] == urun_anahtari]
            
            # Magaza bazinda grupla
            magaza_gruplari = urun_data.groupby('Depo Adi').agg({
                'Satis': 'sum',
                'Envanter': 'sum',
                'Urun Adi': 'first',
                'Renk Aciklamasi': 'first',
                'Beden': 'first',
                'Urun Kodu': 'first'
            }).reset_index()

            # En az 2 magazada olmali transfer icin
            if len(magaza_gruplari) < 2:
                continue

            # Her magaza icin STR hesapla
            magaza_str_listesi = []
            for _, magaza_grup in magaza_gruplari.iterrows():
                magaza = magaza_grup['Depo Adi']
                
                # Istisna magaza kontrolu (extra guvenlik)
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
                    'urun_adi': magaza_grup['Urun Adi'],
                    'renk': magaza_grup.get('Renk Aciklamasi', ''),
                    'beden': magaza_grup.get('Beden', ''),
                    'urun_kodu': magaza_grup['Urun Kodu']
                })

            # En az 2 magaza kaldiysa devam et
            if len(magaza_str_listesi) < 2:
                continue

            # STR'a gore sirala (dusukten yuksege)
            magaza_str_listesi.sort(key=lambda x: x['str'])
            
            # En dusuk ve en yuksek STR'i al
            en_dusuk_str = magaza_str_listesi[0]
            en_yuksek_str = magaza_str_listesi[-1]

            # Transfer kosullarini kontrol et - Strategy parametreli
            kosul_sonuc, kosul_mesaj = self.transfer_kosullari_kontrol(
                en_dusuk_str['satis'], en_dusuk_str['envanter'], 
                en_yuksek_str['satis'], en_yuksek_str['envanter'],
                strategy
            )
            
            if kosul_sonuc:
                # STR bazli transfer miktarini hesapla - Strategy parametreli
                transfer_miktari, str_detaylar = self.str_bazli_transfer_hesapla(
                    en_dusuk_str['satis'], en_dusuk_str['envanter'],
                    en_yuksek_str['satis'], en_yuksek_str['envanter'],
                    strategy
                )
                
                if transfer_miktari > 0:
                    # Stok durumu STR bazinda
                    alan_str_val = str_detaylar['alan_str']
                    if alan_str_val >= 80:
                        stok_durumu = 'YUKSEK'
                    elif alan_str_val >= 50:
                        stok_durumu = 'NORMAL'
                    elif alan_str_val >= 20:
                        stok_durumu = 'DUSUK'
                    else:
                        stok_durumu = 'KRITIK'
                    
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
                # Transfer gerekmeyen urunleri kaydet
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

        # STR farkina gore sirala (yuksek fark = daha oncelikli)
        transferler.sort(key=lambda x: x['str_farki'], reverse=True)

        logger.info(f"Global analiz tamamlandi ({strategy}): {len(transferler)} transfer, {len(transfer_gereksiz)} red")
        if excluded_stores:
            logger.info(f"Istisna magazalar: {excluded_stores}")

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
        'available_strategies': list(STRATEGY_CONFIG.keys())
    })

@app.route('/upload', methods=['POST'])
def upload_file():
    """Dosya yukleme endpoint'i"""
    try:
        logger.info("File upload request received")
        
        if 'file' not in request.files:
            return jsonify({'error': 'Dosya secilmedi'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Dosya secilmedi'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Gecersiz dosya formati! Excel (.xlsx, .xls) veya CSV dosyasi yukleyin.'}), 400
        
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
            return jsonify({'error': f'Dosya okuma hatasi: {str(e)}'}), 400
        
        # Sisteme yukle
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
        return jsonify({'error': f'Dosya yukleme hatasi: {str(e)}'}), 500

@app.route('/remove-file', methods=['POST'])
def remove_file():
    """Yuklenen dosyayi kaldirma endpoint'i"""
    try:
        logger.info("File removal request received")
        
        if sistem.data is None:
            logger.warning("No file to remove")
            return jsonify({'error': 'Kaldirilacak dosya yok'}), 400
        
        # Tum veriyi temizle
        success = sistem.clear_all_data()
        
        if success:
            logger.info("File and all data removed successfully")
            return jsonify({
                'success': True,
                'message': 'Dosya ve tum veriler basariyla kaldirildi',
                'timestamp': datetime.now().isoformat()
            })
        else:
            logger.error("Failed to remove file and data")
            return jsonify({'error': 'Dosya kaldirma islemi basarisiz'}), 500
            
    except Exception as e:
        logger.error(f"File removal error: {str(e)}")
        return jsonify({'error': f'Dosya kaldirma hatasi: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze_data():
    """Transfer analizi - Global, Targeted veya Size Completion"""
    try:
        logger.info("Analysis request received")
        
        if sistem.data is None:
            logger.warning("No data available for analysis")
            return jsonify({'error': 'Once bir dosya yukleyin'}), 400
        
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
        
        # Excluded stores'lari mevcut magaza listesiyle filtrele
        valid_excluded_stores = [store for store in excluded_stores if store in sistem.magazalar]
        
        logger.info(f"Starting {transfer_type} analysis... Strategy: {strategy}")
        logger.info(f"Target store: {target_store}")
        logger.info(f"Excluded stores: {valid_excluded_stores}")
        
        # Transfer tipine gore analiz yap
        if transfer_type == 'size_completion':
            if not target_store or target_store not in sistem.magazalar:
                return jsonify({'error': 'Beden tamamlama icin gecerli bir hedef magaza secin'}), 400
            
            # Beden tamamlama icin strateji kullanilmaz
            results = sistem.beden_tamamlama_analizi_yap(target_store, valid_excluded_stores)
        
        elif transfer_type == 'targeted':
            if not target_store or target_store not in sistem.magazalar:
                return jsonify({'error': 'Spesifik magaza analizi icin gecerli bir hedef magaza secin'}), 400
            
            results = sistem.targeted_transfer_analizi_yap(target_store, strategy, valid_excluded_stores)
        
        else:  # global
            results = sistem.global_transfer_analizi_yap(strategy, valid_excluded_stores)
        
        if results:
            sistem.mevcut_analiz = results
            
            # Sonuclari limitli sekilde gonder (JSON boyutunu kucult)
            limited_results = {
                'analiz_tipi': results['analiz_tipi'],
                'strateji': results['strateji'],
                'target_store': results.get('target_store'),
                'excluded_stores': results.get('excluded_stores', []),
                'excluded_count': results.get('excluded_count', 0),
                'transferler': results['transferler'][:50] if results.get('transferler') else [],
                'toplam_transfer_sayisi': len(results.get('transferler', []))
            }
            
            # Global analiz icin ek bilgiler
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
            return jsonify({'error': 'Analiz basarisiz'}), 500
            
    except Exception as e:
        logger.error(f"Analysis error: {str(e)}")
        return jsonify({'error': f'Analiz hatasi: {str(e)}'}), 500

@app.route('/export/excel', methods=['POST'])
def export_excel():
    """Excel export - Transfer tipine gore ozellestirilmis"""
    try:
        logger.info("Excel export request received")
        
        if not sistem.mevcut_analiz:
            logger.warning("No analysis results available for export")
            return jsonify({'error': 'Analiz sonucu bulunamadi'}), 400
        
        transferler = sistem.mevcut_analiz['transferler']
        analiz_tipi = sistem.mevcut_analiz.get('analiz_tipi', 'global')
        strategy = sistem.mevcut_analiz.get('strateji', 'sakin')
        target_store = sistem.mevcut_analiz.get('target_store', '')
        
        logger.info(f"Exporting {len(transferler)} transfers to Excel (Type: {analiz_tipi}, Strategy: {strategy})")
        
        # Excel dosyasi olustur
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Ana transfer sayfasi
            if transferler:
                df_transfer = pd.DataFrame(transferler)
                
                # Temel sutunlar
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
                
                # Mevcut sutunlari filtrele
                available_columns = {k: v for k, v in selected_columns.items() if k in df_transfer.columns}
                df_export = df_transfer[list(available_columns.keys())].copy()
                df_export = df_export.rename(columns=available_columns)
                
                df_export.to_excel(writer, index=False, sheet_name=sheet_name[:31])  # Excel sheet name limit
            
            # Analiz ozeti sayfasi
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
        
        # Dosya adi olustur
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if analiz_tipi == 'size_completion':
            filename = f'beden_tamamlama_{target_store}_{strategy}_{timestamp}.xlsx'
        elif analiz_tipi == 'targeted':
            filename = f'targeted_{target_store}_{strategy}_{timestamp}.xlsx'
        else:
            filename = f'global_transfer_{strategy}_{timestamp}.xlsx'
        
        # Dosya adini temizle (Windows uyumlulugu icin)
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
        return jsonify({'error': f'Export hatasi: {str(e)}'}), 500

@app.route('/stores', methods=['GET'])
def get_stores():
    """Magaza listesi"""
    try:
        if not sistem.magazalar:
            return jsonify({'error': 'Magaza verisi bulunamadi'}), 400
        
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
        return jsonify({'error': f'Magaza verisi hatasi: {str(e)}'}), 500

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
        return jsonify({'error': f'Strateji verisi hatasi: {str(e)}'}), 500

# Error handlers
@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'Dosya boyutu cok buyuk (max 100MB)'}), 413

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Sunucu hatasi'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint bulunamadi'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug_mode = os.environ.get('FLASK_ENV') != 'production'
    
    logger.info(f"Starting RetailFlow API v6.0 on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
