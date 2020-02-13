# -*- coding: utf-8 -*-
'''
Created on 13 f�vr. 2020

@author: martin.schoreisz
Moduel d'import des donnees MMM
'''

import geopandas as gp


#importer les donn�es et convertir les noms de champs
fichier_src=gp.read_file(r'D:\temp\Linearisation_BM\C19SA0101\C19SA0101\Doc_travail\Donnees_source\MMM\2017_Matin_PR\2017_HPM_link.SHP')
#d�finir les colonnes d�finitives (PLUS TARD ON FERA UN FILTRE EN AMONT)
liste_col_def=['NO', 'FROMNODENO', 'TONODENO', 'TYPENO', 'TSYSSET', 'LENGTH', 'NUMLANES', 'CAPPRT', 'V0_TV', 'VOLVEHPR~1', 
'Q_HC_PL', 'Q_HC_TV', 'Q_HC_VL', 'Q_HPM_PL', 'Q_HPM_TV', 'Q_HPM_VL', 'Q_HPS_PL', 'Q_HPS_TV', 'Q_HPS_VL', 'Q_Jour_PL', 'Q_Jour_TV',
'Q_Jour_VL','coef_HC_PL','coef_HC_TV','coef_HC_VL','Ocup_VL_PL','V0_PL','V0_VL','VCharg_PL','VCharg_VL','VOLPCUP~22',
'SHAREHGV', 'VOLVEH_~23', 'VOLVEH_~24', 'VOLCAPR~25', 'VEHHOUR~26', 'SPEEDLIMIT',
'R_NO', 'R_FROMNODENO', 'R_TONODENO', 'R_TYPENO', 'R_TSYSSET','R_LENGTH', 'R_NUMLANES', 'R_CAPPRT', 'r_V0_TV', 'R_VOLVEHPR~1', 
'R_Q_HC_PL', 'R_Q_HC_TV', 'R_Q_HC_VL', 'R_Q_HPM_PL', 'R_Q_HPM_TV', 'R_Q_HPM_VL', 
'R_Q_HPS_PL', 'R_Q_HPS_TV', 'R_Q_HPS_VL', 'R_Q_Jour_PL', 'R_Q_Jour_TV', 'R_Q_Jour_VL', 'R_coef_HC_PL', 'R_coef_HC_TV', 'R_coef_HC_VL', 'R_Ocup_VL_PL', 'R_V0_PL', 
'R_V0_VL', 'R_VCharg_PL', 'R_VCharg_VL', 'R_VOLPCUP~22', 'R_SHAREHGV', 'R_VOLVEH_~23', 'R_VOLVEH_~24', 'R_VOLCAPR~25', 'R_VEHHOUR~26', 'R_SPEEDLIMIT']
#creer un dico de renomage et renommer
dico_renomage={a: b for a, b in zip(fichier_src.columns,liste_col_def)}
fichier_src.rename(columns=dico_renomage, inplace=True)
#ne conserver dans un premier temps que les attributs relatifs au trafic sur la journ�e
fichier_src_simpl=fichier_src[['geometry','NO', 'FROMNODENO', 'TONODENO', 'TYPENO', 'TSYSSET', 'LENGTH', 'NUMLANES', 'CAPPRT', 'V0_TV', 
             'VOLVEHPR~1']+[a for a in fichier_src.columns if 'Jour' in a ]].copy()
#caculer le tmja_TV
fichier_src_simpl['tmja_tv']=fichier_src_simpl.Q_Jour_TV+fichier_src_simpl.R_Q_Jour_TV