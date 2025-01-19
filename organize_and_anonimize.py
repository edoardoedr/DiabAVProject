"""
Script to organize and anonymize the DiabAV dataset.

This script reads patient data from an Excel file and organizes the dataset
into a structured format based on patient ID, eye (left or right), and date of acquisition.
It then anonymizes the data by removing any personally identifiable information.

The organized structure is as follows:
- Patient ID
  - Patient ID left eye (sx) and right eye (dx)
    - Date of acquisition
      - Measurement files (mc, diabXXXX, aoct)

The script also creates a new Excel file with the cleaned and anonymized patient information.

Usage:
    python organize_and_anonymize.py
"""
import os
import pandas as pd
from tqdm.auto import tqdm
import shutil
import random
from datetime import datetime

class organize_raw_dataset:
    def __init__(self, raw_dataset_path, cleaned_dataset_path):
        self.raw_dataset_path = raw_dataset_path
        self.cleaned_dataset_path = cleaned_dataset_path 
        self.dataset_info, self.lista_pazienti, self.lista_pazienti_giusta = self._get_info_dataset_from_excel()
        self.lista_files = os.listdir(os.path.join(self.raw_dataset_path, 'data'))
        self.lista_pazienti_dx = list(map(lambda x: x+" dx", self.lista_pazienti))
        self.lista_pazienti_sx = list(map(lambda x: x+" sx", self.lista_pazienti))
        
    def _get_info_dataset_from_excel(self):
        """
        Read the patient information from the Excel file.
        """
        dataset_info = pd.read_excel(os.path.join(self.raw_dataset_path, 'raw_info_dataset.xlsx'),
                                    sheet_name = "Foglio1",
                                    header=0,
                                    index_col = None,
                                    engine='openpyxl')
        
        colonne = list(dataset_info.columns)
        dataset_info[colonne[0]] = dataset_info[colonne[0]].map(lambda x: x.rstrip())
        dataset_info[colonne[0]] = dataset_info[colonne[0]].map(lambda x: x.lstrip())
        dataset_info[colonne[0]] = dataset_info[colonne[0]].map(lambda x: x.casefold())
        dataset_info[colonne[1]] = dataset_info[colonne[1]].map(lambda x: x.rstrip())
        dataset_info[colonne[1]] = dataset_info[colonne[1]].map(lambda x: x.lstrip())
        dataset_info[colonne[1]] = dataset_info[colonne[1]].map(lambda x: x.casefold())
        dataset_info[colonne[4]] = dataset_info[colonne[4]].map(lambda x: x.casefold())
        dataset_info[colonne[4]] = dataset_info[colonne[4]].map(lambda x: x.replace(" ", ""))

        dataset_info['occhio'] = dataset_info['occhio'].map(lambda x: x.lower().strip())
        dataset_info['occhio'] = dataset_info['occhio'].map(lambda x: 'destro' if x.startswith('d') else 'sinistro')
        
        lista_pazienti = dataset_info[colonne[4]].values.tolist()
        lista_nomi = dataset_info[colonne[1]].values.tolist()
        lista_cognomi = dataset_info[colonne[0]].values.tolist()
        lista_pazienti_giusta = dataset_info[colonne[4]].values.tolist()

        for i in range(0, len(lista_pazienti) - 1, 1):
            nmgn = lista_nomi[i] + lista_cognomi[i]
            nmgn_succ = lista_nomi[i + 1] + lista_cognomi[i + 1]

            if nmgn == nmgn_succ:
                lista_pazienti_giusta[i+1] = lista_pazienti_giusta[i]
        dataset_info.insert(loc=4, column="Lista Pazienti giusta", value=lista_pazienti_giusta)
        
        return dataset_info, lista_pazienti, lista_pazienti_giusta
        
    def organize_dataset(self):
        
        for occhio_dx, occhio_sx, paziente_giusto in tqdm(zip(self.lista_pazienti_dx, self.lista_pazienti_sx, self.lista_pazienti_giusta), total=len(self.lista_pazienti_dx), desc="copying..."):
            
            elementi_paziente_dx = [file for file in self.lista_files if occhio_dx in file]
            elementi_paziente_sx = [file for file in self.lista_files if occhio_sx in file]
            date_paziente = list(set(file.split(" ")[2] for file in elementi_paziente_dx + elementi_paziente_sx))
            
            for data in date_paziente:
                base_path_dx = os.path.join(self.cleaned_dataset_path, 'data', paziente_giusto, f"{paziente_giusto} dx", data)
                base_path_sx = os.path.join(self.cleaned_dataset_path, 'data', paziente_giusto, f"{paziente_giusto} sx", data)
                os.makedirs(os.path.join(base_path_dx, "DIAB2"), exist_ok=True)
                os.makedirs(os.path.join(base_path_sx, "DIAB2"), exist_ok=True)
                
                
            for file_dx in elementi_paziente_dx:
                data = file_dx.split(" ")[2]
                base_path_dx = os.path.join(self.cleaned_dataset_path, 'data', paziente_giusto, f"{paziente_giusto} dx", data)
                if os.path.isdir(os.path.join(self.raw_dataset_path, 'data', file_dx)):
                    shutil.copytree(os.path.join(self.raw_dataset_path, 'data', file_dx), os.path.join(base_path_dx, file_dx))
                else:
                    dest_path = os.path.join(base_path_dx, "DIAB2" if "diab2" in file_dx else "", file_dx)
                    shutil.copy(os.path.join(self.raw_dataset_path, 'data', file_dx), dest_path)

            for file_sx in elementi_paziente_sx:
                data = file_sx.split(" ")[2]
                base_path_sx = os.path.join(self.cleaned_dataset_path, 'data', paziente_giusto, f"{paziente_giusto} sx", data)
                if os.path.isdir(os.path.join(self.raw_dataset_path, 'data', file_sx)):
                    shutil.copytree(os.path.join(self.raw_dataset_path, 'data', file_sx), os.path.join(base_path_sx, file_sx))
                else:
                    dest_path = os.path.join(base_path_sx, "DIAB2" if "diab2" in file_sx else "", file_sx)
                    shutil.copy(os.path.join(self.raw_dataset_path, 'data', file_sx), dest_path)
        
        self.dataset_info.to_excel(os.path.join(self.cleaned_dataset_path, 'info_dataset.xlsx'), index=False)

class anonimize:
    def __init__(self, dataset_path):
        self.dataset_path = dataset_path
        self.dataset_info = pd.read_excel(os.path.join(self.dataset_path, 'info_dataset.xlsx'), engine='openpyxl', index_col=None, header=0)
        self.lista_folder = os.listdir(os.path.join(self.dataset_path, 'data'))
        self.lista_folder_anonima = self.anonimize_paz()
        
    def anonimize_dataset(self):
        
        for paz, paz_anom in tqdm(zip(self.lista_folder, self.lista_folder_anonima), total=len(self.lista_folder), desc="Anonimizing..."):
            
            folder_occhio_dx = os.path.join(self.dataset_path, 'data', paz, f"{paz} dx")
            folder_occhio_sx = os.path.join(self.dataset_path, 'data', paz, f"{paz} sx")
            date_dx = [os.path.join(folder_occhio_dx, data) for data in os.listdir(folder_occhio_dx)]
            date_sx = [os.path.join(folder_occhio_sx, data) for data in os.listdir(folder_occhio_sx)]
            
            for dt_dx in date_dx:
                self.rename_pz(dt_dx, paz_anom)
                
            for dt_sx in date_sx:
                self.rename_pz(dt_sx, paz_anom)

            #rinonimo le cartelle occhio_dx e occhio_sx
            os.rename(folder_occhio_dx, os.path.join(self.dataset_path, 'data', paz, 'occhio_dx'))
            os.rename(folder_occhio_sx, os.path.join(self.dataset_path, 'data', paz, 'occhio_sx'))
            os.rename(os.path.join(self.dataset_path, 'data', paz), os.path.join(self.dataset_path, 'data', paz_anom))
            
        self.export_manifest_dataset()
            
    def export_manifest_dataset(self):        
        
        manifest_dict_list = []
        
        for paz in self.lista_folder_anonima:
            folder_paz = os.path.join(self.dataset_path, 'data', paz)
            folder_occhio_dx = os.path.join(folder_paz, 'occhio_dx')
            folder_occhio_sx = os.path.join(folder_paz, 'occhio_sx')
            date_dx = sorted([os.path.join(folder_occhio_dx, data) for data in os.listdir(folder_occhio_dx)], key=self._extract_date)
            date_sx = sorted([os.path.join(folder_occhio_sx, data) for data in os.listdir(folder_occhio_sx)], key=self._extract_date)
            
            
            for dt_dx in date_dx:
                data_misura = datetime.strptime(os.path.basename(dt_dx), "%d%m%Y")
                path_files_in_folder = self._check_folder_data(dt_dx, paz)
                label_dict = self.get_labels_from_excel(paz, 'destro', data_misura)
                
                if all(path == 'path_missing' for path in path_files_in_folder.values()) and all(label == 'label_missing' for label in label_dict.values()):
                    continue
                else:
                    manifest_dict = {'Patient ID': paz, 'eye': 'dx', 'date': data_misura}
                        
                    manifest_dict.update(path_files_in_folder)
                    manifest_dict.update(label_dict)
                    manifest_dict_list.append(manifest_dict)
                    
            for dt_sx in date_sx:
                data_misura = datetime.strptime(os.path.basename(dt_sx), "%d%m%Y")
                path_files_in_folder = self._check_folder_data(dt_sx, paz)
                label_dict = self.get_labels_from_excel(paz, 'sinistro', data_misura)
                
                if all(path == 'path_missing' for path in path_files_in_folder.values()) and all(label == 'label_missing' for label in label_dict.values()):
                    continue
                else:
                    manifest_dict = {'Patient ID': paz, 'eye': 'sx', 'date': data_misura}
                        
                    manifest_dict.update(path_files_in_folder)
                    manifest_dict.update(label_dict)
                    manifest_dict_list.append(manifest_dict)
            
                    
        df_manifest = pd.DataFrame(manifest_dict_list)
        #print(df_manifest.head())
        df_manifest.to_csv(os.path.join(self.dataset_path, 'manifest_dataset.csv'), index=False)
   
    def rename_pz(self, fold, paz_anom):
        
        for im_fol in os.listdir(fold):
            file_path = os.path.join(fold, im_fol)
            
            if os.path.isfile(file_path):
                if "mc" in im_fol:
                    new_name = f"{paz_anom}_mc.tif"
                elif "diab1" in im_fol:
                    new_name = f"{paz_anom}_diab1.tif"
                elif "aoct" in im_fol:
                    new_name = f"{paz_anom}_aoct.tif"
                else:
                    continue
                os.rename(file_path, os.path.join(fold, new_name))
            
            elif os.path.isdir(file_path):
                if "aoct" in im_fol:
                    ls_aoct = os.listdir(file_path)
                    for aoct in ls_aoct:
                        if " OCT " in aoct:
                            new_name = aoct.split("OCT", 1)[1].split("v", 1)[0].strip().replace("  ", "_").replace(" ", "_")
                            os.rename(os.path.join(file_path, aoct), os.path.join(file_path, f"{paz_anom}_{new_name}_OCT.png"))
                        elif " OCTA " in aoct:
                            new_name = aoct.split("OCTA", 1)[1].split("v", 1)[0].strip().replace("  ", "_").replace(" ", "_")
                            os.rename(os.path.join(file_path, aoct), os.path.join(file_path, f"{paz_anom}_{new_name}_OCTA.png"))
                    os.rename(file_path, os.path.join(fold, f"{paz_anom}_aoct"))
                elif "DIAB2" in im_fol:
                    ls_diab = sorted(os.listdir(file_path))
                    for diab in ls_diab:
                        os.rename(os.path.join(file_path, diab), os.path.join(file_path, f"{paz_anom}_{diab.split(' ')[-1]}"))
                    os.rename(file_path, os.path.join(fold, f"{paz_anom}_diab2"))

    def _clean_excel(self):
        
        self.dataset_info['cognome'] = self.dataset_info['cognome'].map(lambda x: x.lower().strip())
        self.dataset_info['nome'] = self.dataset_info['nome'].map(lambda x: x.lower().strip())
        self.dataset_info['occhio'] = self.dataset_info['occhio'].map(lambda x: x.lower().strip())
        self.dataset_info['occhio'] = self.dataset_info['occhio'].map(lambda x: 'destro' if x.startswith('d') else 'sinistro')

    def anonimize_paz(self):
        
        self._clean_excel()
        lista_folder_anonima = ["PZ" + str(i).zfill(4) for i in range(len(self.lista_folder))]
        random.shuffle(lista_folder_anonima)
        lista_pazienti_excel = self.dataset_info["Lista Pazienti giusta"].values.tolist()
        new_paziente = self.dataset_info["Lista Pazienti giusta"].values.tolist()
        
        pazienti_mancanti = [pz for pz in lista_pazienti_excel if pz not in self.lista_folder]
        print(f"Missing patients: {pazienti_mancanti}, please check the dataset.")
        
        if pazienti_mancanti:
            self.dataset_info = self.dataset_info[~self.dataset_info["Lista Pazienti giusta"].isin(pazienti_mancanti)]
            lista_pazienti_excel = self.dataset_info["Lista Pazienti giusta"].values.tolist()
            new_paziente = self.dataset_info["Lista Pazienti giusta"].values.tolist()
        
        for i, pz in enumerate(lista_pazienti_excel):
            
            if pz not in pazienti_mancanti:
                indice = self.lista_folder.index(pz)
                new_paziente[i] = lista_folder_anonima[indice]

        self.dataset_info.insert(loc=5, column="Lista Pazienti anonima", value=new_paziente)
        self.dataset_info.to_excel(os.path.join(self.dataset_path, 'info_dataset.xlsx'), index=False)
        
        return lista_folder_anonima
   
    def logger(self, message):
        log_file = os.path.join(self.dataset_path, 'anonimize_log.txt')
        with open(log_file, 'a') as f:
            f.write(message + '\n')
    
    def _check_folder_data(self, folder, paz_id):
        
        path_fundus_image = os.path.join(folder, f"{paz_id}_mc.tif")
        path_diab1_image = os.path.join(folder, f"{paz_id}_diab1.tif")
        path_aoct_image = os.path.join(folder, f"{paz_id}_aoct.tif")
        path_diab2_folder = os.path.join(folder, f"{paz_id}_diab2")
        path_aoct_folder = os.path.join(folder, f"{paz_id}_aoct")
        
        image_list = [path_fundus_image, path_diab1_image, path_aoct_image]
        image_list_name = ['path_fundus_image', 'path_diab1_image', 'path_aoct_image']
        image_list = [path if os.path.isfile(path) else 'path_missing' for path in image_list]
        folder_list = [path_diab2_folder, path_aoct_folder]
        folder_list_name = ['path_diab2_folder', 'path_aoct_folder']
        folder_list = [path if os.path.isdir(path) and len(os.listdir(path)) > 0 else 'path_missing' for path in folder_list]
        
        return dict(zip(image_list_name + folder_list_name, image_list + folder_list))
    
    def get_labels_from_excel(self, paz, occhio, data):
        
        paziente = self.dataset_info[self.dataset_info["Lista Pazienti anonima"] == paz]
        paziente = paziente[paziente["occhio"].str.strip() == occhio]
        
        if paziente.empty == True:
            return {'diab': 'label_missing', 'acuity': 'label_missing'}
        
        date_excel = [paziente['data esame'].values, paziente['mese 3 data'].values, paziente['m6 data'].values]
        date_excel = [pd.to_datetime(date[0]) if date.size > 0 else None for date in date_excel]
        
        tipologia_diabete = paziente["diabete (1 sano, 2 diabete senza edema in,  3 edema lieve, 4 grave)"].values[0]
        acuita_visiva = paziente["acuità visiva (snellen)"].values[0]
        
        if pd.isna(tipologia_diabete) == True:
            tipologia_diabete = "label_missing"
        
        if pd.isna(acuita_visiva) == True or not isinstance(acuita_visiva, str):
            acuita_visiva = "label_missing"
        
        if data == date_excel[1]:
            if pd.isna(paziente["CAMBIO STADIO"].values[0]) == False:
                tipologia_diabete = paziente["CAMBIO STADIO"].values[0]
            if pd.isna(paziente["m3 AV"].values[0]) == False:
                acuita_visiva = paziente["m3 AV"].values[0]
        elif data == date_excel[2]:
            if pd.isna(paziente["CAMBIO STADIO.1"].values[0]) == False:
                tipologia_diabete = paziente["CAMBIO STADIO.1"].values[0]
            if pd.isna(paziente["m6 av"].values[0]) == False:
                acuita_visiva = paziente["m6 av"].values[0]
        
        return {'diab': tipologia_diabete, 'acuity': acuita_visiva}
    
    @staticmethod
    def _extract_date(folder_name):
        date_str = os.path.basename(folder_name)
        return datetime.strptime(date_str, "%d%m%Y")

        
if __name__ == "__main__":
    raw_dataset_path = "/home/edofroses/DiabAVProject/Dataset_compressed/DiabAV_original_v0"
    cleaned_dataset_path = "/home/edofroses/DiabAVProject/Datasets/DiabAV_anonimized_v0"
    
    organizer = organize_raw_dataset(raw_dataset_path, cleaned_dataset_path)
    organizer.organize_dataset()
    
    anonimizer = anonimize(cleaned_dataset_path)
    anonimizer.anonimize_dataset()
    
    print("Dataset organized and anonymized successfully!")